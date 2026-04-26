const fs = require("fs");
const path = require("path");
const moment = require('moment-timezone');
const axios = require("axios");
const Users = require("./controllers/users.js");
const Threads = require("./controllers/threads.js");
const Currencies = require("./controllers/currencies.js");
const logger = require("../utils/log.js");
const config = require("../../config.json");

// ======== CACHE SYSTEM - GIẢM GỌI API ========
const globalCache = {
    users: new Map(),
    threads: new Map(),
    threadInfo: new Map(),
    timestamp: new Map(),
    CACHE_TIME: 3 * 60 * 60 * 1000, // 3 giờ
    
    set(type, key, value) {
        this[type].set(key, value);
        this.timestamp.set(`${type}_${key}`, Date.now());
    },
    
    get(type, key) {
        const now = Date.now();
        const cached = this[type].get(key);
        const timestamp = this.timestamp.get(`${type}_${key}`);
        
        if (cached && timestamp && (now - timestamp < this.CACHE_TIME)) {
            return cached;
        }
        return null;
    },
    
    has(type, key) {
        return this.get(type, key) !== null;
    },
    
    clear(type, key) {
        if (key) {
            this[type].delete(key);
            this.timestamp.delete(`${type}_${key}`);
        } else {
            this[type].clear();
        }
    }
};

// Rate Limiter - Tránh spam request
const rateLimiter = {
    lastCall: {},
    minDelay: 1000, // 1 giây giữa các lần gọi
    
    async wait(key) {
        const now = Date.now();
        const last = this.lastCall[key] || 0;
        const timeSince = now - last;
        
        if (timeSince < this.minDelay) {
            await new Promise(resolve => setTimeout(resolve, this.minDelay - timeSince));
        }
        
        this.lastCall[key] = Date.now();
    }
};

// ======== BẮT ĐẦU MODULE ========
module.exports = function ({ api, models }) {
    const users = Users({ models, api });
    const threads = Threads({ models, api });
    const currencies = Currencies({ models });

    const checkttDataPath = path.join(process.cwd(), '../../modules/commands/checktt/');

    // Wrapper cho getNameUser với cache
    const getCachedUserName = async (userID) => {
        // Kiểm tra cache trước
        if (globalCache.has('users', userID)) {
            return globalCache.get('users', userID);
        }
        
        // Nếu không có trong cache, gọi API
        await rateLimiter.wait(`user_${userID}`);
        const userName = await users.getNameUser(userID) || 'Facebook User';
        
        // Lưu vào cache
        globalCache.set('users', userID, userName);
        return userName;
    };

    // Wrapper cho getThreadInfo với cache
    const getCachedThreadInfo = async (threadID) => {
        // Kiểm tra cache trước
        if (globalCache.has('threadInfo', threadID)) {
            return globalCache.get('threadInfo', threadID);
        }
        
        // Nếu không có trong cache, gọi API
        await rateLimiter.wait(`thread_${threadID}`);
        const threadInfo = await threads.getInfo(threadID);
        
        // Lưu vào cache
        globalCache.set('threadInfo', threadID, threadInfo);
        return threadInfo;
    };

    // SetInterval cập nhật tương tác ngày/tuần
    let day = moment.tz("Asia/Ho_Chi_Minh").day();
    setInterval(async () => {
        const dayNow = moment.tz("Asia/Ho_Chi_Minh").day();
        if (day !== dayNow) {
            day = dayNow;
            const checkttData = fs.readdirSync(checkttDataPath);
            console.log('--> CHECKTT: Ngày Mới');
            
            for (const checkttFile of checkttData) {
                const checktt = JSON.parse(fs.readFileSync(path.join(checkttDataPath, checkttFile)));
                let storage = [], count = 1;
                
                // Batch get names - gọi 1 lần cho nhiều user
                const userIDs = checktt.day.map(item => item.id);
                for (const userID of userIDs) {
                    const item = checktt.day.find(i => i.id === userID);
                    const userName = await getCachedUserName(userID); // Dùng cache
                    storage.push({ ...item, name: userName });
                }
                
                storage.sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
                const timechecktt = moment.tz('Asia/Ho_Chi_Minh').format('DD/MM/YYYY || HH:mm:ss');
                const haha = `\n────────────────────\n💬 Tổng tin nhắn: ${storage.reduce((a, b) => a + b.count, 0)}\n⏰ Time: ${timechecktt}\n✏️ Các bạn khác cố gắng tương tác nếu muốn lên top nha`;
                let checkttBody = '[ TOP TƯƠNG TÁC NGÀY ]\n────────────────────\n📝 Top 10 người tương tác nhiều nhất hôm qua:\n\n';
                checkttBody += storage.slice(0, 10).map(item => `${count++}. ${item.name} - 💬 ${item.count} tin nhắn`).join('\n');
                
                api.sendMessage(
                    { body: checkttBody + haha, attachment: global.khanhdayr ? global.khanhdayr.splice(0, 1) : [] },
                    checkttFile.replace('.json', ''),
                    err => err && console.log(err)
                );
                
                for (const e of checktt.day) e.count = 0;
                checktt.time = dayNow;
                fs.writeFileSync(path.join(checkttDataPath, checkttFile), JSON.stringify(checktt, null, 4));
            }
            
            if (dayNow === 1) {
                console.log('--> CHECKTT: Tuần Mới');
                for (const checkttFile of checkttData) {
                    const checktt = JSON.parse(fs.readFileSync(path.join(checkttDataPath, checkttFile)));
                    let storage = [], count = 1;
                    
                    // Batch get names
                    const userIDs = checktt.week.map(item => item.id);
                    for (const userID of userIDs) {
                        const item = checktt.week.find(i => i.id === userID);
                        const userName = await getCachedUserName(userID); // Dùng cache
                        storage.push({ ...item, name: userName });
                    }
                    
                    storage.sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
                    const tctt = moment.tz('Asia/Ho_Chi_Minh').format('DD/MM/YYYY || HH:mm:ss');
                    const dzvcl = `\n────────────────────\n⏰ Time: ${tctt}\n✏️ Các bạn khác cố gắng tương tác nếu muốn lên top nha`;
                    let checkttBody = '[ TOP TƯƠNG TÁC TUẦN ]\n────────────────────\n📝 Top 10 người tương tác nhiều nhất tuần qua:\n\n';
                    checkttBody += storage.slice(0, 10).map(item => `${count++}. ${item.name} - 💬 ${item.count} tin nhắn`).join('\n');
                    
                    api.sendMessage(
                        { body: checkttBody + dzvcl, attachment: global.khanhdayr ? global.khanhdayr.splice(0, 1) : [] },
                        checkttFile.replace('.json', ''),
                        err => err && console.log(err)
                    );
                    
                    for (const e of checktt.week) e.count = 0;
                    fs.writeFileSync(path.join(checkttDataPath, checkttFile), JSON.stringify(checktt, null, 4));
                }
            }
            if (global.client) global.client.sending_top = false;
        }
    }, 1000 * 10);

    // Push biến từ database lên global
    (async function () {
        try {
            logger(global.getText('listen', 'startLoadEnvironment'), '[ DATABASE ]');
            let threadsData = await threads.getAll(),
                usersData = await users.getAll(['userID', 'name', 'data']),
                currenciesData = await currencies.getAll(['userID']);
            
            for (const data of threadsData) {
                const idThread = String(data.threadID);
                global.data.allThreadID.push(idThread);
                global.data.threadData.set(idThread, data['data'] || {});
                global.data.threadInfo.set(idThread, data.threadInfo || {});
                
                // Lưu vào cache luôn
                globalCache.set('threadInfo', idThread, data.threadInfo || {});
                
                if (data['data'] && data['data']['banned'])
                    global.data.threadBanned.set(idThread, {
                        'reason': data['data']['reason'] || '',
                        'dateAdded': data['data']['dateAdded'] || ''
                    });
                if (data['data'] && data['data']['commandBanned'] && data['data']['commandBanned'].length !== 0)
                    global['data']['commandBanned']['set'](idThread, data['data']['commandBanned']);
                if (data['data'] && data['data']['NSFW']) global['data']['threadAllowNSFW']['push'](idThread);
            }
            
            logger.loader(global.getText('listen', 'loadedEnvironmentThread'));
            
            for (const dataU of usersData) {
                const idUsers = String(dataU['userID']);
                global.data['allUserID']['push'](idUsers);
                
                if (dataU.name && dataU.name.length !== 0) {
                    global.data.userName.set(idUsers, dataU.name);
                    // Lưu vào cache luôn
                    globalCache.set('users', idUsers, dataU.name);
                }
                
                if (dataU.data && dataU.data.banned == 1) global.data['userBanned']['set'](idUsers, {
                    'reason': dataU['data']['reason'] || '',
                    'dateAdded': dataU['data']['dateAdded'] || ''
                });
                if (dataU['data'] && dataU.data['commandBanned'] && dataU['data']['commandBanned'].length !== 0)
                    global['data']['commandBanned']['set'](idUsers, dataU['data']['commandBanned']);
            }
            
            for (const dataC of currenciesData) global.data.allCurrenciesID.push(String(dataC['userID']));
        } catch (error) {
            logger.loader(global.getText('listen', 'failLoadEnvironment', error), 'error');
        }
    })();

    const admin = config.ADMINBOT;
    logger("┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓", "[ PCODER ]");
    for (let i = 0; i < admin.length; i++) {
        logger(` ID ADMIN ${i + 1}: ${admin[i] || "Trống"}`, "[ PCODER ]");
    }
    logger(` ID BOT: ${api.getCurrentUserID()}`, "[ PCODER ]");
    logger(` PREFIX: ${global.config.PREFIX}`, "[ PCODER ]");
    logger(` NAME BOT: ${global.config.BOTNAME || "Mirai - PCODER"}`, "[ PCODER ]");
    logger("┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛", "[ PCODER ]");

    const handleCommand = require("./handle/handleCommand.js")({ api, models, Users: users, Threads: threads, Currencies: currencies });
    const handleCommandEvent = require("./handle/handleCommandEvent.js")({ api, models, Users: users, Threads: threads, Currencies: currencies });
    const handleReply = require("./handle/handleReply.js")({ api, models, Users: users, Threads: threads, Currencies: currencies });
    const handleReaction = require("./handle/handleReaction.js")({ api, models, Users: users, Threads: threads, Currencies: currencies });
    const handleEvent = require("./handle/handleEvent.js")({ api, models, Users: users, Threads: threads, Currencies: currencies });
    const handleRefresh = require("./handle/handleRefresh.js")({ api, models, Users: users, Threads: threads, Currencies: currencies });
    const handleCreateDatabase = require("./handle/handleCreateDatabase.js")({ api, Threads: threads, Users: users, Currencies: currencies, models });

    logger.loader(`Ping load source code: ${Date.now() - global.client.timeStart}ms`);

    // =================================================================
    // ============ HỆ THỐNG ANTI NÂNG CAO - BẮT ĐẦU =====================
    // =================================================================

    class Catbox {
        async uploadImage(url) {
            try {
                const response = await axios.get(`https://catbox-mnib.onrender.com/upload?url=${encodeURIComponent(url)}`);
                if (response.data?.url) return response.data.url;
                throw new Error("Catbox không trả về URL");
            } catch (error) {
                console.error("[CATBOX ERROR]", error.message);
                return null;
            }
        }
    }

    const antiPath = path.join(process.cwd(), 'modules/commands/cache/anti_settings.json');
    const chongCuopBoxDir = path.join(process.cwd(), 'modules/commands/cache/chongcuopbox');

    const getThreadData = (threadID) => {
        try {
            if (!fs.existsSync(antiPath)) fs.writeFileSync(antiPath, JSON.stringify({}));
            return JSON.parse(fs.readFileSync(antiPath, 'utf8'))[threadID] || {};
        } catch (e) { console.error("[ANTI HELPER ERROR]", e); return {}; }
    };

    const saveThreadData = (threadID, data) => {
        try {
            const allSettings = JSON.parse(fs.readFileSync(antiPath, 'utf8'));
            allSettings[threadID] = { ...(allSettings[threadID] || {}), ...data };
            fs.writeFileSync(antiPath, JSON.stringify(allSettings, null, 4));
            
            // Clear cache khi có thay đổi settings
            globalCache.clear('threadInfo', threadID);
        } catch (e) { console.error("[ANTI HELPER ERROR]", e); }
    };

    const readJSON = (filePath) => {
        const fullPath = path.join(chongCuopBoxDir, filePath);
        try {
            if (!fs.existsSync(fullPath)) fs.writeFileSync(fullPath, JSON.stringify({}));
            return JSON.parse(fs.readFileSync(fullPath, 'utf8'));
        } catch (e) { return {}; }
    };

    const writeJSON = (filePath, data) => {
        const fullPath = path.join(chongCuopBoxDir, filePath);
        if (!fs.existsSync(path.dirname(fullPath))) {
            fs.mkdirSync(path.dirname(fullPath), { recursive: true });
        }
        fs.writeFileSync(fullPath, JSON.stringify(data, null, 4));
    };

    const logAction = (threadID, message) => {
        const logDir = path.join(chongCuopBoxDir, "logs");
        if (!fs.existsSync(logDir)) fs.mkdirSync(logDir, { recursive: true });
        const logFilePath = path.join(logDir, `${threadID}.log`);
        const timestamp = new Date().toLocaleString("vi-VN", { timeZone: "Asia/Ho_Chi_Minh" });
        fs.appendFileSync(logFilePath, `[${timestamp}] ${message}\n`);
    };

    async function restoreGroupImage(api, threadID, settings, kickedUserName = null) {
        try {
            await rateLimiter.wait(`restore_image_${threadID}`); // Rate limit
            
            let imageStream;
            
            if (settings.groupImagePath && fs.existsSync(settings.groupImagePath)) {
                imageStream = fs.createReadStream(settings.groupImagePath);
            } else if (settings.groupImage) {
                const response = await axios.get(settings.groupImage, { responseType: 'stream' });
                imageStream = response.data;
            } else {
                console.error("[ANTI-IMAGE] Không tìm thấy ảnh cũ để khôi phục.");
                api.sendMessage("⚠️ Lỗi: Không tìm thấy ảnh cũ để khôi phục. QTV vui lòng dùng lệnh để reset anti!", threadID);
                return;
            }

            api.changeGroupImage(imageStream, threadID, (err) => {
                if (err) {
                    console.error("[ANTI-IMAGE ERROR] Lỗi khôi phục ảnh:", err);
                    api.sendMessage("❌ Không thể khôi phục ảnh nhóm! Vui lòng liên hệ admin.", threadID);
                } else {
                    const message = kickedUserName 
                        ? `🚫 Đã kick ${kickedUserName} vì tự ý đổi ảnh nhóm và khôi phục ảnh cũ!`
                        : "✅ Đã khôi phục ảnh nhóm cũ!";
                    api.sendMessage(message, threadID);
                    console.log(`[ANTI-IMAGE] Khôi phục ảnh nhóm ${threadID} thành công.`);
                }
            });
        } catch (e) {
            console.error(`[ANTI-IMAGE ERROR] Lỗi khôi phục ảnh:`, e.message);
            api.sendMessage("❌ Lỗi khi khôi phục ảnh nhóm!", threadID);
        }
    }

    async function initAntiSettings(api, threadID) {
        try {
            await rateLimiter.wait(`init_anti_${threadID}`); // Rate limit
            
            const threadInfo = await api.getThreadInfo(threadID);
            const currentImage = threadInfo.imageSrc;
            
            if (currentImage) {
                const imagePath = path.join(process.cwd(), 'modules/commands/cache/anti_images', `${threadID}.png`);
                const imageDir = path.dirname(imagePath);
                
                if (!fs.existsSync(imageDir)) {
                    fs.mkdirSync(imageDir, { recursive: true });
                }
                
                const response = await axios.get(currentImage, { responseType: 'stream' });
                response.data.pipe(fs.createWriteStream(imagePath));
                
                saveThreadData(threadID, {
                    groupName: threadInfo.threadName,
                    groupImage: currentImage,
                    groupImagePath: imagePath
                });
                
                console.log(`[INIT-ANTI] Đã lưu thông tin nhóm ${threadID}.`);
            }
        } catch (e) {
            console.error("[INIT-ANTI ERROR]", e);
        }
    }

    // =================================================================
    // ============= HỆ THỐNG ANTI NÂNG CAO - KẾT THÚC ==================
    // =================================================================

    const spamTracker = {};

    // ========= LẮNG NGHE SỰ KIỆN =========
    return async function listen(event) {
        try {
            const { threadID, logMessageType, logMessageData, author, body, senderID, mentions, type } = event;
            const { ADMINBOT = [], BOTNAME, PREFIX } = global.config;
            const botID = api.getCurrentUserID();

            // ============ XỬ LÝ HỆ THỐNG ANTI ============
            if (author && author !== botID) {
                const settings = getThreadData(threadID);
                if (settings && Object.values(settings).some(v => v === true)) {
                    
                    // Dùng cache thay vì gọi API mỗi lần
                    const threadInfo = await getCachedThreadInfo(threadID);
                    const isBotAdmin = (userID) => ADMINBOT.includes(userID);
                    const isGroupAdmin = (userID) => threadInfo.adminIDs.some(item => item.id == userID);

                    if (!isBotAdmin(author)) {
                        switch (logMessageType) {
                            case "log:thread-name":
                                if (settings.antiChangeGroupName) {
                                    if (isGroupAdmin(author)) {
                                        saveThreadData(threadID, { groupName: logMessageData?.name || "Tên Nhóm Cũ" });
                                        console.log(`[ANTI-NAME] QTV đổi tên nhóm thành công.`);
                                    } else {
                                        const userName = await getCachedUserName(author); // Dùng cache
                                        
                                        await rateLimiter.wait(`kick_${threadID}`); // Rate limit
                                        api.removeUserFromGroup(author, threadID, (err) => {
                                            if (err) {
                                                console.error("[ANTI-NAME KICK ERROR]", err);
                                                api.setTitle(settings.groupName || "Tên Nhóm Cũ", threadID, (err) => {
                                                    if (err) console.error("[ANTI-NAME ERROR]", err);
                                                    else api.sendMessage(`⚠️ ${userName} - Mày không có quyền đổi tên nhóm!`, threadID);
                                                });
                                            } else {
                                                console.log(`[ANTI-NAME] Đã kick ${userName} (${author}) vì đổi tên nhóm.`);
                                                api.setTitle(settings.groupName || "Tên Nhóm Cũ", threadID, (err) => {
                                                    if (err) console.error("[ANTI-NAME ERROR]", err);
                                                    else api.sendMessage(`🚫 Đã kick ${userName} vì tự ý đổi tên nhóm!`, threadID);
                                                });
                                            }
                                        });
                                    }
                                }
                                break;

                            case "change_thread_image":
                            case "log:thread-icon":
                            case "log:thread-image":
                                if (settings.antiChangeGroupImage) {
                                    if (isGroupAdmin(author)) {
                                        console.log(`[ANTI-IMAGE] QTV đổi ảnh nhóm, đang cập nhật...`);
                                        setTimeout(async () => {
                                            try {
                                                await rateLimiter.wait(`update_image_${threadID}`); // Rate limit
                                                
                                                const newThreadInfo = await api.getThreadInfo(threadID);
                                                const newImageUrl = newThreadInfo.imageSrc;
                                                
                                                if (newImageUrl) {
                                                    const imagePath = path.join(process.cwd(), 'modules/commands/cache/anti_images', `${threadID}.png`);
                                                    const imageDir = path.dirname(imagePath);
                                                    
                                                    if (!fs.existsSync(imageDir)) {
                                                        fs.mkdirSync(imageDir, { recursive: true });
                                                    }
                                                    
                                                    const response = await axios.get(newImageUrl, { responseType: 'stream' });
                                                    response.data.pipe(fs.createWriteStream(imagePath));
                                                    
                                                    saveThreadData(threadID, { 
                                                        groupImage: newImageUrl,
                                                        groupImagePath: imagePath 
                                                    });
                                                    console.log(`[ANTI-IMAGE] Đã cập nhật ảnh nhóm mới.`);
                                                }
                                            } catch (e) {
                                                console.error("[ANTI-IMAGE ERROR] Lỗi khi cập nhật ảnh:", e);
                                            }
                                        }, 3000);
                                    } else {
                                        const userName = await getCachedUserName(author); // Dùng cache
                                        
                                        await rateLimiter.wait(`kick_${threadID}`); // Rate limit
                                        api.removeUserFromGroup(author, threadID, (err) => {
                                            if (err) {
                                                console.error("[ANTI-IMAGE KICK ERROR]", err);
                                                restoreGroupImage(api, threadID, settings).catch(console.error);
                                            } else {
                                                console.log(`[ANTI-IMAGE] Đã kick ${userName} (${author}) vì đổi ảnh nhóm.`);
                                                setTimeout(() => restoreGroupImage(api, threadID, settings, userName).catch(console.error), 2000);
                                            }
                                        });
                                    }
                                }
                                break;

                            case "log:user-nickname": {
                                const targetID = logMessageData?.participant_id;

                                if (targetID === botID) {
                                    await rateLimiter.wait(`nickname_${threadID}`); // Rate limit
                                    api.changeNickname(`『 ${PREFIX} 』⪼ ${BOTNAME}`, threadID, botID, (err) => {
                                        if (err) console.error("[ANTI-NICKNAME BOT ERROR]", err);
                                    });
                                    break;
                                }

                                if (settings.antiChangeNickname && targetID) {
                                    const oldNickname = (settings.nicknames || {})[targetID] || "";
                                    
                                    if (!oldNickname) {
                                        const newNicknames = settings.nicknames || {};
                                        newNicknames[targetID] = logMessageData?.nickname || "";
                                        saveThreadData(threadID, { nicknames: newNicknames });
                                        break;
                                    }

                                    await rateLimiter.wait(`nickname_${threadID}`); // Rate limit
                                    api.changeNickname(oldNickname, threadID, targetID, (err) => {
                                        if (err) console.error("[ANTI-NICKNAME ERROR]", err);
                                        else if (author !== botID) {
                                            api.sendMessage(`Không ai được phép tự đổi biệt danh. Bot đã khôi phục lại biệt danh cũ.`, threadID);
                                        }
                                    });
                                }
                                break;
                            }

                            case "log:unsubscribe":
                                if (settings.antiOut && logMessageData?.leftParticipantFbId === author) {
                                    await rateLimiter.wait(`add_user_${threadID}`); // Rate limit
                                    api.addUserToGroup(author, threadID, async (err) => {
                                        if (err) console.error("[ANTI-OUT ERROR]", err);
                                        else {
                                            const name = await getCachedUserName(author); // Dùng cache
                                            api.sendMessage(`Định out chùa à thằng lồn ${name}? Mày nghĩ mày thoát được tao à?`, threadID);
                                        }
                                    });
                                }
                                break;

                            case "log:subscribe":
                                if (settings.antiJoin && !isGroupAdmin(author) && logMessageData?.addedParticipants) {
                                    for (const member of logMessageData.addedParticipants) {
                                        if (member.userFbId !== botID && member.userFbId !== author) {
                                            await rateLimiter.wait(`kick_${threadID}`); // Rate limit
                                            api.removeUserFromGroup(member.userFbId, threadID, (err) => {
                                                if (err) console.error("[ANTI-JOIN ERROR]", err);
                                                else api.sendMessage(`Thêm cái lồn gì? Không có sự cho phép của QTV, cút!`, threadID);
                                            });
                                        }
                                    }
                                }
                                break;

                            case "log:thread-admins":
                                if (settings.antiQTV && logMessageData) {
                                    const { ADMIN_EVENT, TARGET_ID } = logMessageData;
                                    if (ADMIN_EVENT === "add_admin") {
                                        const blacklist = readJSON("blacklist.json");
                                        if (blacklist[TARGET_ID]) {
                                            await rateLimiter.wait(`admin_${threadID}`); // Rate limit
                                            api.changeAdminStatus(threadID, TARGET_ID, false);
                                            api.sendMessage(`⚠️ Thằng này trong danh sách đen, thêm làm QTV con cặc! Đã gỡ.`, threadID);
                                            return;
                                        }
                                        const monitoring = readJSON("monitoring.json");
                                        if (!monitoring[threadID]) monitoring[threadID] = {};
                                        monitoring[threadID][TARGET_ID] = Date.now() + 48 * 60 * 60 * 1000;
                                        writeJSON("monitoring.json", monitoring);
                                        const targetName = await getCachedUserName(TARGET_ID); // Dùng cache
                                        api.sendMessage(`🔐 QTV mới "${targetName}"`, threadID);
                                    } else if (ADMIN_EVENT === "remove_admin") {
                                        const monitoring = readJSON("monitoring.json");
                                        if (monitoring[threadID]?.[author] && Date.now() < monitoring[threadID][author]) {
                                            const authorName = await getCachedUserName(author); // Dùng cache
                                            const targetName = await getCachedUserName(TARGET_ID); // Dùng cache
                                            
                                            await rateLimiter.wait(`admin_${threadID}`); // Rate limit
                                            api.changeAdminStatus(threadID, author, false);
                                            api.changeAdminStatus(threadID, TARGET_ID, true);
                                            const blacklist = readJSON("blacklist.json");
                                            blacklist[author] = { reason: `Gỡ QTV (${targetName}) khi đang bị giám sát`, timestamp: Date.now() };
                                            writeJSON("blacklist.json", blacklist);
                                            delete monitoring[threadID][author];
                                            writeJSON("monitoring.json", monitoring);
                                            api.sendMessage(`❗️ VI PHẠM ❗️\nThằng ngu "${authorName}" vừa gỡ QTV của "${targetName}" khi đang bị theo dõi.\n\n=> Hậu quả: Mất quyền QTV, vô blacklist mà ngồi.`, threadID);
                                            logAction(threadID, `VIOLATION: ${author} gỡ quyền ${TARGET_ID}. Đã xử lý.`);
                                        }
                                    }
                                }
                                break;
                        }
                    }
                }
            }
            
            // Xử lý anti link, tag, spam
            if ((type === "message" || type === "message_reply") && body) {
                const settings = getThreadData(threadID);
                if (settings && !ADMINBOT.includes(senderID) && senderID !== botID) {
                    const threadInfo = await getCachedThreadInfo(threadID); // Dùng cache
                    const isGroupAdmin = threadInfo.adminIDs.some(i => i.id == senderID);
                    
                    if (!isGroupAdmin) {
                        // Anti Link
                        if (settings.antiLink && /(http(s)?:\/\/.)/i.test(body)) {
                            const senderName = await getCachedUserName(senderID); // Dùng cache
                            await rateLimiter.wait(`kick_${threadID}`); // Rate limit
                            api.removeUserFromGroup(senderID, threadID, (err) => {
                                if (err) return console.error(`[ANTI-LINK ERROR]`, err);
                                api.sendMessage(`Thằng ml ${senderName} gửi link vớ vẩn, tao kick vỡ mõm.`, threadID);
                            });
                        }
                        
                        // Anti Tag
                        if (settings.antiTag) {
                            const mentionCount = mentions ? Object.keys(mentions).length : 0;
                            if (mentionCount >= 10 || /@mọi người|@everyone|@all/i.test(body)) {
                                const senderName = await getCachedUserName(senderID); // Dùng cache
                                await rateLimiter.wait(`kick_${threadID}`); // Rate limit
                                api.removeUserFromGroup(senderID, threadID, (err) => {
                                    if (err) return console.error(`[ANTI-TAG ERROR]`, err);
                                    api.sendMessage(`Thằng lồn ${senderName} tag all con cặc, kick cho đỡ ngứa mắt.`, threadID);
                                });
                            }
                        }

                        // Anti Spam
                        if (settings.antiSpam) {
                            const config = settings.antiSpamConfig || { count: 5, time: 10 };
                            const now = Date.now();
                            const timeWindow = config.time * 1000;

                            if (!spamTracker[threadID]) spamTracker[threadID] = {};
                            
                            if (!spamTracker[threadID][senderID]) {
                                spamTracker[threadID][senderID] = { count: 1, timestamp: now };
                            } else {
                                const userData = spamTracker[threadID][senderID];
                                if (now - userData.timestamp > timeWindow) {
                                    userData.count = 1;
                                    userData.timestamp = now;
                                } else {
                                    userData.count++;
                                    
                                    if (userData.count > config.count) {
                                        const senderName = await getCachedUserName(senderID); // Dùng cache
                                        await rateLimiter.wait(`kick_${threadID}`); // Rate limit
                                        api.removeUserFromGroup(senderID, threadID, (err) => {
                                            if (err) return console.error(`[ANTI-SPAM ERROR]`, err);
                                            api.sendMessage(`Thằng ml ${senderName} spam vcl (${userData.count} tin/${config.time}s), kick vỡ mõm.`, threadID);
                                        });
                                        delete spamTracker[threadID][senderID];
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (error) {
            console.error("[ANTI CRITICAL ERROR]", error);
        }

        // --- PHẦN CODE GỐC CỦA BẠN ---
        const { threadID, senderID } = event;
        const threadData = global.data.threadData.get(threadID) || {};
        const prefix = threadData.PREFIX || global.config.PREFIX;

        function toMMDDYYYY(dateStr) {
            if (!dateStr || typeof dateStr !== "string") return "";
            const parts = dateStr.split("/");
            if (parts.length !== 3) return "";
            return `${parts[1]}/${parts[0]}/${parts[2]}`;
        }

        const RENT_PRICE = 50000;
        const RENEW_PRICE = RENT_PRICE * 0.95;

        // Kiểm tra thuê bot
        if (
            (event.body || '').startsWith(prefix) &&
            event.senderID != api.getCurrentUserID() &&
            !global.config.ADMINBOT.includes(event.senderID) &&
            !global.config.NDH.includes(event.senderID)
        ) {
            let thuebot = [];
            const rentDataPath = path.join(process.cwd(), 'modules/commands/cache/data_rentbot_pro/thuebot_pro.json');
            
            try {
                if (fs.existsSync(rentDataPath)) {
                    thuebot = JSON.parse(fs.readFileSync(rentDataPath, 'utf8'));
                }
            } catch (e) {
                console.error("[RENT_CHECK] Lỗi đọc hoặc parse file thuebot_pro.json:", e);
                thuebot = [];
            }

            const command = (event.body || '').slice(prefix.length).trim().split(/\s+/)[0].toLowerCase();
            const find_thuebot = thuebot.find($ => $.t_id == event.threadID);
            
            const allowedCommands = ['rent', 'bankqr', 'callad', 'ff', 'ffrank', 'box', 'verify', 'setkey'];

            if (!allowedCommands.includes(command)) {
                
                if (!find_thuebot) {
                    return api.sendMessage(
                        `⚠️ Nhóm của bạn chưa thuê bot.\nVui lòng sử dụng lệnh "${prefix}rent thanhtoan" để thuê.`,
                        event.threadID,
                        event.messageID
                    );
                }

                const timeEnd = find_thuebot.time_end;
                const now = require('moment-timezone')().tz('Asia/Ho_Chi_Minh');
                
                const endDate = require('moment-timezone')(timeEnd, 'DD/MM/YYYY', true).endOf('day');

                if (!endDate.isValid() || now.isAfter(endDate)) {
                    return api.sendMessage(
                        `📅 Gói thuê bot của nhóm đã hết hạn.\nVui lòng sử dụng lệnh "${prefix}rent giahan" để gia hạn.`,
                        event.threadID,
                        event.messageID
                    );
                }
            }
        }

        let gio = moment.tz('Asia/Ho_Chi_Minh').format('DD/MM/YYYY || HH:mm:ss');
        let thu = moment.tz('Asia/Ho_Chi_Minh').format('dddd');
        const thuVI = {
            Sunday: 'Chủ nhật',
            Monday: 'Thứ hai',
            Tuesday: 'Thứ ba',
            Wednesday: 'Thứ tư',
            Thursday: 'Thứ năm',
            Friday: 'Thứ sáu',
            Saturday: 'Thứ bảy'
        };
        thu = thuVI[thu] || thu;

        // Xử lý các loại event
        switch (event.type) {
            case "message":
            case "message_reply":
            case "message_unsend":
                handleCreateDatabase({ event });
                handleCommand({ event });
                handleReply({ event });
                handleCommandEvent({ event });
                break;
            case "event":
                handleEvent({ event });
                handleRefresh({ event });
                break;
            case "message_reaction":
                handleReaction({ event });
                break;
            default:
                break;
        }
    };
};
