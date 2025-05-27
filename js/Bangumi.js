// --- 小部件配置 ---
const WidgetConfig = {
    MAX_CONCURRENT_DETAILS_FETCH: 8, // 最大并发详情获取数
    MAX_CONCURRENT_TMDB_SEARCHES: 4, // 最大并发TMDB搜索数
    MAX_CONCURRENT_CALENDAR_ENHANCE: 3, // 最大并发日历项增强数
    MAX_CONCURRENT_TMDB_FULL_DETAILS_FETCH: 3, // 最大并发TMDB完整详情获取数
    HTTP_RETRIES: 1, // HTTP请求重试次数
    HTTP_MAIN_RETRIES: 2, // 主要HTTP请求（非预取）重试次数
    HTTP_RETRY_DELAY: 1000, // HTTP重试延迟（毫秒）

    // 禁用获取详细的 TMDB 简介、具体分类、标语等信息，通过减少 TMDB API 调用显著加快加载速度。*****
    FETCH_FULL_TMDB_DETAILS: false,
    TMDB_APPEND_TO_RESPONSE: "translations,genres", // TMDB完整详情请求附加内容（仅在 FETCH_FULL_TMDB_DETAILS 为 true 时相关）

    TMDB_SEARCH_STAGE1_YEAR_STRICT_SCORE_BOOST: 12, // TMDB搜索第一阶段（严格年份）分数加成
    TMDB_SEARCH_STAGE1_HIGH_CONFIDENCE_EXIT_SCORE: 23, // TMDB搜索第一阶段高置信度退出分数
    TMDB_SEARCH_MIN_SCORE_THRESHOLD: 6, // TMDB搜索最小匹配分数阈值

    CACHE_TTL_MS: 25 * 60 * 1000, // 默认缓存有效期（毫秒）
    PREFETCH_CACHE_TTL_MS: 2 * 60 * 1000, // 预取缓存有效期（毫秒）
    MAX_PREFETCHED_PAGES: 5, // 最大预取页面数

    DEBUG_LOGGING: false, // 是否开启调试日志

    BGM_BASE_URL: "https://bgm.tv", // Bangumi 基础URL
    BGM_API_USER_AGENT: "", // Bangumi API 用户代理

    TTL_TRENDS_MS: 6 * 60 * 60 * 1000, // 热门榜单数据有效期
    TTL_RANK_MS: 24 * 60 * 60 * 1000, // 普通排行数据有效期
    TTL_SEASON_EARLY_MS: 12 * 60 * 60 * 1000, // 季度早期数据有效期
    TTL_SEASON_LATE_MS: 3 * 24 * 60 * 60 * 1000, // 季度后期数据有效期
    TTL_ARCHIVE_MS: 7 * 24 * 60 * 60 * 1000, // 历史存档数据有效期
    TTL_CALENDAR_API_MS: 6 * 60 * 60 * 1000, // 日历API数据有效期
    TTL_CALENDAR_ITEM_ENHANCED_MS: 24 * 60 * 60 * 1000, // 日历条目增强数据有效期
    TTL_BGM_DETAIL_COVER_MS: 7 * 24 * 60 * 60 * 1000, // BGM详情页封面有效期
    TTL_TMDB_FULL_DETAIL_MS: 24 * 60 * 60 * 1000, // TMDB完整详情API响应缓存有效期

    SEASON_EARLY_WEEKS: 6, // 定义早期季度的的周数

    MAX_TOTAL_TMDB_QUERIES_TO_PROCESS: 4, // TMDB搜索时处理的最大查询词数量

    TMDB_ANIMATION_GENRE_ID: 16 // TMDB动画类型ID
};

var WidgetMetadata = {
    id: "bangumi_charts_tmdb_v3",
    title: "Bangumi 热门榜单",
    description: "从Bangumi获取近期热门、每日放送数据，支持榜单筛选，智能匹配TMDB数据。",
    author: "Autism ",
    site: "https://github.com/opix-maker/Forward",
    version: "1.0.3", 
    requiredVersion: "0.0.1",
    modules: [
        {
            title: "近期热门",
            description: "按作品类型浏览近期热门内容 (固定按热度 trends 排序)",
            requiresWebView: false,
            functionName: "fetchRecentHot",
            params: [
                { name: "category", title: "分类", type: "enumeration", value: "anime", enumOptions: [ { title: "动画", value: "anime" }, { title: "书籍", value: "book" }, { title: "音乐", value: "music" }, { title: "游戏", value: "game" }, { title: "三次元", value: "real" } ] },
                { name: "page", title: "页码", type: "page", value: "1" }
            ]
        },
        {
            title: "放送/发售时段排行",
            description: "按年份、季度/全年及作品类型浏览排行",
            requiresWebView: false,
            functionName: "fetchAirtimeRanking",
            params: [
                { name: "category", title: "分类", type: "enumeration", value: "anime", enumOptions: [ { title: "动画", value: "anime" }, { title: "书籍", value: "book" }, { title: "音乐", value: "music" }, { title: "游戏", value: "game" }, { title: "三次元", value: "real" } ] },
                { name: "year", title: "年份", type: "input", description: "例如: 2024。留空则浏览所有年份。" },
                { name: "month", title: "月份/季度", type: "enumeration", value: "all", description: "选择全年或特定季度对应的月份。留空则为全年。", enumOptions: [ { title: "全年", value: "all" }, { title: "冬季 (1月)", value: "1" }, { title: "春季 (4月)", value: "4" }, { title: "夏季 (7月)", value: "7" }, { title: "秋季 (10月)", value: "10" } ] },
                { name: "sort", title: "排序方式", type: "enumeration", value: "rank", enumOptions: [ { title: "排名", value: "rank" }, { title: "热度", value: "trends" }, { title: "收藏数", value: "collects" }, { title: "发售日期", value: "date" }, { title: "名称", "value": "title" } ] },
                { name: "page", title: "页码", type: "page", value: "1" }
            ]
        },
        {
            title: "每日放送",
            description: "查看指定范围的放送（数据来自Bangumi API）",
            requiresWebView: false,
            functionName: "fetchDailyCalendarApi",
            params: [
                {
                    name: "filterType",
                    title: "筛选范围",
                    type: "enumeration",
                    value: "today",
                    enumOptions: [
                        { title: "今日放送", value: "today" },
                        { title: "指定单日", value: "specific_day" },
                        { title: "本周一至四", value: "mon_thu" },
                        { title: "本周五至日", value: "fri_sun" },
                        { title: "整周放送", value: "all_week" }
                    ]
                },
                {
                    name: "specificWeekday",
                    title: "选择星期",
                    type: "enumeration",
                    value: "1",
                    description: "仅当筛选范围为“指定单日”时有效。",
                    enumOptions: [
                        { title: "星期一", value: "1" }, { title: "星期二", value: "2" },
                        { title: "星期三", value: "3" }, { title: "星期四", value: "4" },
                        { title: "星期五", value: "5" }, { title: "星期六", value: "6" },
                        { title: "星期日", value: "7" }
                    ],
                    belongTo: { paramName: "filterType", value: ["specific_day"] }
                },
                {
                    name: "dailySortOrder", title: "排序方式", type: "enumeration",
                    value: "popularity_rat_bgm",
                    description: "对每日放送结果进行排序",
                    enumOptions: [
                        { title: "热度(评分人数)", value: "popularity_rat_bgm" },
                        { title: "评分", value: "score_bgm_desc" },
                        { title: "放送日(更新日期)", value: "airdate_desc" },
                        { title: "默认", value: "default" }
                    ]
                },
                {
                    name: "dailyRegionFilter", title: "地区筛选", type: "enumeration", value: "all",
                    description: "筛选特定地区的放送内容 (主要依赖TMDB数据)",
                    enumOptions: [
                        { title: "全部地区", value: "all" },
                        { title: "日本", value: "JP" },
                        { title: "中国大陆", value: "CN" },
                        { title: "欧美", value: "US_EU" },
                        { title: "其他/未知", value: "OTHER" }
                    ]
                }
            ]
        }
    ]
};

WidgetConfig.BGM_API_USER_AGENT = `ForwardWidget/1.2 (${WidgetMetadata.id}) (https://github.com/InchStudio/ForwardWidgets)`;

const CONSTANTS = {
    SCRIPT_VERSION: WidgetMetadata.version,
    LOG_PREFIX_GENERAL: `[BGM_TMDB_WIDGET v${WidgetMetadata.version}]`,
    CACHE_KEYS: {
        TMDB_SEARCH: `tmdb_search_computed_v${WidgetMetadata.version}`,
        ITEM_DETAIL_COMPUTED: `item_detail_computed_v${WidgetMetadata.version}_final`,
        BGM_CALENDAR_API: `bgm_calendar_api_data_v${WidgetMetadata.version}`,
        CALENDAR_ITEM_FINAL_DISPLAY: `calendar_item_final_display_v${WidgetMetadata.version}`,
        BGM_DETAIL_COVER: `bgm_detail_cover_v${WidgetMetadata.version}`,
        TMDB_FULL_DETAIL: `tmdb_full_detail_v${WidgetMetadata.version}`
    },
    MEDIA_TYPES: {
        TV: "tv",
        MOVIE: "movie",
        ANIME: "anime",
        BOOK: "book",
        MUSIC: "music",
        GAME: "game",
        REAL: "real"
    },
    TMDB_ANIMATION_GENRE_ID: WidgetConfig.TMDB_ANIMATION_GENRE_ID,
    BGM_API_TYPE_MAPPING: { 1: "book", 2: "anime", 3: "music", 4: "game", 6: "real" },
    JS_DAY_TO_BGM_API_ID: { 0: 7, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6 },
    REGION_FILTER_US_EU_COUNTRIES: ["US", "GB", "FR", "DE", "CA", "AU", "ES", "IT"]
};

// --- 缓存工具 ---
const CacheUtil = {
    cache: new Map(),
    pendingPromises: new Map(),
    _generateKey: function(type, identifier) {
        if (typeof identifier === 'object' && identifier !== null) {
            try {
                return `${type}_${JSON.stringify(Object.keys(identifier).sort().reduce((obj, key) => { obj[key] = identifier[key]; return obj; }, {}))}`;
            } catch (e) {
                if (WidgetConfig.DEBUG_LOGGING) console.warn(`${CONSTANTS.LOG_PREFIX_GENERAL} [缓存工具] 生成对象标识符缓存键失败:`, identifier, e.message);
                let fallbackKeyPart = '';
                if (typeof identifier === 'object' && !Array.isArray(identifier)) {
                    try {
                        Object.keys(identifier).sort().forEach(k => {
                            fallbackKeyPart += `_${k}:${String(identifier[k])}`;
                        });
                        if (fallbackKeyPart) return `${type}${fallbackKeyPart}`;
                    } catch (fallbackError) {
                         if (WidgetConfig.DEBUG_LOGGING) console.warn(`${CONSTANTS.LOG_PREFIX_GENERAL} [缓存工具] 回退缓存键生成失败:`, fallbackError.message);
                    }
                }
                console.error(`${CONSTANTS.LOG_PREFIX_GENERAL} [缓存工具] 严重错误: 使用可能非唯一的备用缓存键，标识符为:`, identifier);
                return `${type}_${String(identifier)}_ERROR_POTENTIALLY_NON_UNIQUE`;
            }
        }
        return `${type}_${String(identifier)}`;
    },
    get: function(type, identifier) {
        const key = this._generateKey(type, identifier);
        if (this.pendingPromises.has(key)) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [缓存工具] 等待进行中Promise: ${key.substring(0, 80)}...`);
            return this.pendingPromises.get(key);
        }
        const entry = this.cache.get(key);
        if (entry && Date.now() < entry.expiry) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [缓存工具] 命中缓存: ${key.substring(0, 80)}...`);
            return Promise.resolve(entry.value);
        } else if (entry) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [缓存工具] 缓存过期: ${key.substring(0, 80)}...`);
            this.cache.delete(key);
            this.pendingPromises.delete(key);
        }
        return null;
    },
    set: function(type, identifier, valuePromise, customTtl) {
        const key = this._generateKey(type, identifier);
        this.pendingPromises.set(key, valuePromise);
        const ttlToUse = typeof customTtl === 'number' ? customTtl : WidgetConfig.CACHE_TTL_MS;

        return valuePromise.then(value => {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [缓存工具] 设置缓存: ${key.substring(0, 80)}... (TTL: ${ttlToUse / 1000}s)`);
            this.cache.set(key, { value: value, expiry: Date.now() + ttlToUse });
            this.pendingPromises.delete(key);
            return value;
        }).catch(error => {
            if (WidgetConfig.DEBUG_LOGGING) console.warn(`${CONSTANTS.LOG_PREFIX_GENERAL} [缓存工具] Promise执行失败，从pending移除: ${key.substring(0, 80)}...`, error.message);
            this.pendingPromises.delete(key);
            throw error;
        });
    },
    cachedOrFetch: function(cacheType, identifier, fetchFn, options = {}) {
        const cachedPromise = this.get(cacheType, identifier);
        if (cachedPromise) return cachedPromise;
        
        let ttl = options.ttl;
        if (typeof options.calculateTTL === 'function') {
            ttl = options.calculateTTL(options.ttlIdentifier || identifier, options.context || {});
        }
        return this.set(cacheType, identifier, fetchFn(), ttl);
    }
};

// --- 智能预取缓存 ---
const PrefetchCache = {
    prefetchedHtml: new Map(),
    get: function(url) {
        const entry = this.prefetchedHtml.get(url);
        if (entry && (Date.now() - entry.timestamp < WidgetConfig.PREFETCH_CACHE_TTL_MS)) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [预取缓存] 命中: ${url}`);
            return entry.promise;
        }
        if (entry) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [预取缓存] 过期或无效: ${url}`);
            this.prefetchedHtml.delete(url);
        }
        return null;
    },
    set: function(url, htmlPromise) {
        if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [预取缓存] 开始预取并设置Promise: ${url}`);
        const entry = { promise: htmlPromise, timestamp: Date.now(), inProgress: true };
        this.prefetchedHtml.set(url, entry);

        htmlPromise.finally(() => { 
             const currentEntry = this.prefetchedHtml.get(url);
             if (currentEntry === entry) { 
                currentEntry.inProgress = false;
                htmlPromise.catch(() => {
                    if (this.prefetchedHtml.get(url) === entry) {
                        this.prefetchedHtml.delete(url);
                        if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [预取缓存] 预取失败后删除条目: ${url}`);
                    }
                });
             }
        });
        if (this.prefetchedHtml.size > WidgetConfig.MAX_PREFETCHED_PAGES) {
            let oldestKey = null; let oldestTime = Infinity;
            for (const [key, value] of this.prefetchedHtml.entries()) {
                if (!value.inProgress && value.timestamp < oldestTime) {
                    oldestTime = value.timestamp;
                    oldestKey = key;
                }
            }
            if (oldestKey) {
                this.prefetchedHtml.delete(oldestKey);
                if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [预取缓存] 清理最旧条目: ${oldestKey}`);
            }
        }
        return htmlPromise;
    },
    fetchAndCacheHtml: function(url, headers) {
        let existingEntry = this.prefetchedHtml.get(url);
        if (existingEntry && (existingEntry.inProgress || (Date.now() - existingEntry.timestamp < WidgetConfig.PREFETCH_CACHE_TTL_MS))) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [预取缓存] 使用现有预取Promise: ${url}`);
            return existingEntry.promise;
        }
        if (existingEntry) { 
             this.prefetchedHtml.delete(url);
        }
        const newHtmlPromise = fetchWithRetry(url, { headers }, 'get', false, WidgetConfig.HTTP_RETRIES) 
            .then(response => {
                if (!response?.data) throw new Error(`预取 ${url} 无有效数据`);
                if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [预取缓存] 预取成功，获得HTML: ${url}`);
                return response.data;
            })
            .catch(err => {              
                if (WidgetConfig.DEBUG_LOGGING) console.warn(`${CONSTANTS.LOG_PREFIX_GENERAL} [预取缓存] 预取网络请求失败 ${url}: ${err.message}`);
                throw err;
            });
        return this.set(url, newHtmlPromise); 
    }
};

// --- HTTP请求封装 ---
async function fetchWithRetry(url, options, method = 'get', isTmdb = false, customRetries) {
    let attempts = 0;
    const maxRetries = customRetries !== undefined ? customRetries : WidgetConfig.HTTP_MAIN_RETRIES; 
    const retryDelay = WidgetConfig.HTTP_RETRY_DELAY;

    while (attempts <= maxRetries) {
        try {
            if (WidgetConfig.DEBUG_LOGGING && attempts > 0) {
                console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [HTTP] 第 ${attempts + 1} 次尝试 ${url.substring(0, 80)}...`);
            }
            const api = isTmdb ? Widget.tmdb : Widget.http;
            const response = await api[method](url, options);
            
            if (isTmdb && response && response.data === undefined && typeof response === 'object' && response !== null) {
                 return response; 
            }
            return response; 

        } catch (error) {
            attempts++;
            const isAuthError = String(error.message).includes("401") || String(error.message).includes("403");

            if (WidgetConfig.DEBUG_LOGGING || attempts > maxRetries || isAuthError) {
                console.warn(`${CONSTANTS.LOG_PREFIX_GENERAL} [HTTP] 获取 ${url.substring(0, 80)}... 错误 (尝试 ${attempts}/${maxRetries + 1}):`, error.message);
            }

            if (isAuthError) throw error; 
            if (attempts > maxRetries) throw error; 
            
            const delayMultiplier = attempts; 
            await new Promise(resolve => setTimeout(resolve, retryDelay * delayMultiplier));
        }
    }
    throw new Error(`${CONSTANTS.LOG_PREFIX_GENERAL} [HTTP] 达到最大重试次数 ${url}`); 
}

// --- 辅助函数 ---
function isEarlySeason(year, month, currentDate = new Date()) {
    if (!year || !month || month === 'all' || year === '' || month === '') return false;
    const currentYear = currentDate.getFullYear();
    const seasonYear = parseInt(year, 10);
    const seasonStartMonth = parseInt(month, 10);
    if (isNaN(seasonYear) || isNaN(seasonStartMonth)) return false;
    if (currentYear < seasonYear) return false; 
    const seasonStartDate = new Date(seasonYear, seasonStartMonth - 1, 1);
    const earlySeasonEndDate = new Date(seasonStartDate);
    earlySeasonEndDate.setDate(seasonStartDate.getDate() + WidgetConfig.SEASON_EARLY_WEEKS * 7);
    return currentDate >= seasonStartDate && currentDate <= earlySeasonEndDate;
}

function calculateContentTTL(identifier, context) {
    const { category, year, month, sort } = identifier; 
    const currentDate = context.currentDate || new Date();
    if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TTL计算] 传入标识符:`, identifier);

    if (sort === 'trends') return WidgetConfig.TTL_TRENDS_MS;
    
    if (year && year !== "" && month && month !== "" && month !== 'all') { 
        if (isEarlySeason(year, month, currentDate)) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TTL计算] 使用季度早期TTL for ${year}-${month}`);
            return WidgetConfig.TTL_SEASON_EARLY_MS;
        } else {
            const seasonStartDate = new Date(parseInt(year, 10), parseInt(month, 10) - 1, 1);
            const monthsSinceSeasonStart = (currentDate.getFullYear() - seasonStartDate.getFullYear()) * 12 + (currentDate.getMonth() - seasonStartDate.getMonth());
            if (monthsSinceSeasonStart > 6) { 
                 if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TTL计算] 使用存档TTL for ${year}-${month}`);
                 return WidgetConfig.TTL_ARCHIVE_MS;
            }
            if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TTL计算] 使用季度后期TTL for ${year}-${month}`);
            return WidgetConfig.TTL_SEASON_LATE_MS;
        }
    } else if (year && year !== "") { 
        if (parseInt(year,10) < currentDate.getFullYear() -1) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TTL计算] 使用存档TTL for year ${year}`);
            return WidgetConfig.TTL_ARCHIVE_MS; 
        }
        if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TTL计算] 使用排行TTL for year ${year}`);
        return WidgetConfig.TTL_RANK_MS; 
    }
    if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TTL计算] 使用默认排行TTL`);
    return WidgetConfig.TTL_RANK_MS; 
}

function normalizeTmdbQuery(query) { if (!query || typeof query !== 'string') return ""; return query.toLowerCase().trim().replace(/[\[\]【】（）()「」『』:：\-－_,\.・]/g, ' ').replace(/\s+/g, ' ').trim();}
function getInfoFromBox($, labelText) { let value = '';const listItems = $('#infobox li');for (let i = 0; i < listItems.length; i++) { const liElement = listItems.eq(i); const tipSpan = liElement.find('span.tip').first(); if (tipSpan.text().trim() === labelText) { value = liElement.clone().children('span.tip').remove().end().text().trim(); return value; } } return value; }
function parseDate(dateStr) { if (!dateStr || typeof dateStr !== 'string') return ''; dateStr = dateStr.trim(); let match; match = dateStr.match(/^(\d{4})年(\d{1,2})月(\d{1,2})日/); if (match) return `${match[1]}-${String(match[2]).padStart(2, '0')}-${String(match[3]).padStart(2, '0')}`; match = dateStr.match(/^(\d{4})年(\d{1,2})月(?!日)/); if (match) return `${match[1]}-${String(match[2]).padStart(2, '0')}-01`; match = dateStr.match(/^(\d{4})年(冬|春|夏|秋)/); if (match) { let m = '01'; if (match[2] === '春') m = '04'; else if (match[2] === '夏') m = '07'; else if (match[2] === '秋') m = '10'; return `${match[1]}-${m}-01`; } match = dateStr.match(/^(\d{4})年(?![\d月春夏秋冬])/); if (match) return `${match[1]}-01-01`; match = dateStr.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})/); if (match) return `${match[1]}-${String(match[2]).padStart(2, '0')}-${String(match[3]).padStart(2, '0')}`; match = dateStr.match(/^(\d{4})[-/](\d{1,2})(?!.*[-/])/); if (match) return `${match[1]}-${String(match[2]).padStart(2, '0')}-01`; match = dateStr.match(/^(\d{4})$/); if (match) return `${match[1]}-01-01`; if (WidgetConfig.DEBUG_LOGGING && dateStr) console.warn(`${CONSTANTS.LOG_PREFIX_GENERAL} [日期解析] 无法解析日期字符串: "${dateStr}"`); return '';}

// --- TMDB完整详情填充辅助函数 ---
function populateItemFromTmdbFullDetail(itemRef, tmdbDetail) {
    if (!tmdbDetail) {
        if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB填充工具] 未提供TMDB详情对象（TMDB ID ${itemRef.tmdb_id || 'N/A'}, BGM ID ${itemRef.link?.split('/').pop() || itemRef.id}）。`);
        return;
    }
    itemRef.tmdb_overview = tmdbDetail.overview || itemRef.tmdb_overview || "";
    const currentDescription = String(itemRef.description || "");
    const dayPrefixMatch = currentDescription.match(/^\[.*?\]\s*/);
    const dayPrefix = dayPrefixMatch ? dayPrefixMatch[0] : "";
    const baseDescription = currentDescription.replace(/^\[.*?\]\s*/, '');
    itemRef.description = `${dayPrefix}${tmdbDetail.overview || baseDescription}`.trim();
    if (tmdbDetail.genres?.length > 0) {
        itemRef.tmdb_genres = tmdbDetail.genres.map(g => g.name).join(', ');
        itemRef.genreTitle = itemRef.tmdb_genres;
    }
    itemRef.tmdb_tagline = tmdbDetail.tagline || "";
    itemRef.tmdb_status = tmdbDetail.status || "";
    itemRef.tmdb_original_title = tmdbDetail.original_title || tmdbDetail.original_name || "";
    if (tmdbDetail.origin_country && Array.isArray(tmdbDetail.origin_country) && tmdbDetail.origin_country.length > 0) {
        itemRef.tmdb_origin_countries = tmdbDetail.origin_country;
    } else if (tmdbDetail.production_countries && Array.isArray(tmdbDetail.production_countries) && tmdbDetail.production_countries.length > 0) {
        itemRef.tmdb_origin_countries = tmdbDetail.production_countries.map(pc => pc.iso_3166_1);
    } else if (!itemRef.tmdb_origin_countries || itemRef.tmdb_origin_countries.length === 0) {
        itemRef.tmdb_origin_countries = [];
    }
    if (typeof tmdbDetail.vote_count === 'number') {
        itemRef.tmdb_vote_count = tmdbDetail.vote_count;
    }
    let bestChineseTitleFromTmdb = '';
    if (tmdbDetail.translations?.translations) {
        const chineseTranslation = tmdbDetail.translations.translations.find(
            t => t.iso_639_1 === 'zh' && t.iso_3166_1 === 'CN' && t.data && (t.data.title || t.data.name)
        );
        if (chineseTranslation) {
            bestChineseTitleFromTmdb = (chineseTranslation.data.title || chineseTranslation.data.name).trim();
        }
    }
    itemRef.tmdb_preferred_title = bestChineseTitleFromTmdb || itemRef.title; 
    if (bestChineseTitleFromTmdb && bestChineseTitleFromTmdb !== itemRef.title) {
        if(WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB填充工具] 更新 TMDB ID ${itemRef.tmdb_id} 的主标题为 TMDB 中文翻译: "${bestChineseTitleFromTmdb.substring(0,30)}..." (原 BGM 链接 ID: ${itemRef.link?.split('/').pop() || 'N/A'})`);
        itemRef.title = bestChineseTitleFromTmdb;
    }
    if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB填充工具] 条目 (TMDB ID ${itemRef.tmdb_id}) 已从完整详情填充。`);
}

// --- TMDB评分函数 ---
function scoreTmdbResult(result, query, validYear, searchMediaType, originalTitle, chineseTitle) {
    let currentScore = 0;
    const resultTitleLower = normalizeTmdbQuery(result.title || result.name);
    const resultOriginalTitleLower = normalizeTmdbQuery(result.original_title || result.original_name);
    const queryLower = normalizeTmdbQuery(query);
    const primaryBgmTitleLower = normalizeTmdbQuery(originalTitle || chineseTitle);
    if (resultTitleLower === queryLower || resultOriginalTitleLower === queryLower) {
        currentScore += 15;
        if (primaryBgmTitleLower && (resultTitleLower === primaryBgmTitleLower || resultOriginalTitleLower === primaryBgmTitleLower)) currentScore += 5;
    } else if (resultTitleLower.includes(queryLower) || resultOriginalTitleLower.includes(queryLower)) {
        currentScore += 7;
        if (primaryBgmTitleLower && (resultTitleLower.includes(primaryBgmTitleLower) || resultOriginalTitleLower.includes(primaryBgmTitleLower))) currentScore += 3;
    } else {
        const queryWords = queryLower.split(/\s+/).filter(w => w.length > 1);
        if (queryWords.length > 0) {
            const titleWords = new Set([...resultTitleLower.split(/\s+/), ...resultOriginalTitleLower.split(/\s+/)]);
            let commonWords = 0;
            queryWords.forEach(qw => { if (titleWords.has(qw)) commonWords++; });
            currentScore += (commonWords / queryWords.length) * 6;
        } else { currentScore -= 2; }
    }
    if (validYear) {
        const resDate = result.release_date || result.first_air_date;
        if (resDate && resDate.startsWith(String(validYear).substring(0,4))) {
            const resYear = parseInt(resDate.substring(0, 4), 10);
            const yearDiff = Math.abs(resYear - validYear);
            if (yearDiff === 0) currentScore += 6; else if (yearDiff === 1) currentScore += 3; else if (yearDiff <= 2) currentScore += 1; else currentScore -= (yearDiff * 1.5);
        } else { currentScore -= 2; }
    } else { currentScore += 1; }
    if (result.original_language === 'ja' && (searchMediaType === CONSTANTS.MEDIA_TYPES.TV || searchMediaType === CONSTANTS.MEDIA_TYPES.MOVIE)) currentScore += 2.5;
    currentScore += Math.log10((result.popularity || 0) + 1) * 2.2 + Math.log10((result.vote_count || 0) + 1) * 1.2;
    if (result.adult) currentScore -= 10;
    return currentScore;
}

// --- TMDB搜索查询词生成辅助函数 ---
function generateTmdbSearchQueries(originalTitle, chineseTitle, listTitle) {
    const coreQueries = new Set();
    const refineQueryForSearch = (text) => {
        if (!text || typeof text !== 'string') return "";
        let refined = text.trim();
        refined = refined.replace(/\s*\((\d{4}|S\d{1,2}|Season\s*\d{1,2}|第[一二三四五六七八九十零〇]+[季期部篇章])\)/gi, '');
        refined = refined.replace(/\s*\[(\d{4}|S\d{1,2}|Season\s*\d{1,2}|第[一二三四五六七八九十零〇]+[季期部篇章])\]/gi, '');
        refined = refined.replace(/\s*【(\d{4}|S\d{1,2}|Season\s*\d{1,2}|第[一二三四五六七八九十零〇]+[季期部篇章])】/gi, '');
        return normalizeTmdbQuery(refined);
    };
    const addQueryToList = (text, list) => {
        if (!text || typeof text !== 'string') return;
        const refinedBase = refineQueryForSearch(text);
        if (refinedBase) list.add(refinedBase);
        const originalNormalized = normalizeTmdbQuery(text);
        if (originalNormalized && originalNormalized !== refinedBase) list.add(originalNormalized);
        const firstPartOriginal = normalizeTmdbQuery(text.split(/[:：\-\s（(【\[]/)[0].trim());
        if (firstPartOriginal) list.add(firstPartOriginal);
        const noSeasonSuffix = normalizeTmdbQuery(text.replace(/第.+[期季部篇章]$/g, '').trim());
        if (noSeasonSuffix && noSeasonSuffix !== originalNormalized && noSeasonSuffix !== refinedBase) list.add(noSeasonSuffix);
    };
    addQueryToList(originalTitle, coreQueries);
    addQueryToList(chineseTitle, coreQueries);
    addQueryToList(listTitle, coreQueries);
    [originalTitle, chineseTitle, listTitle].forEach(t => { if (t) { const normalized = normalizeTmdbQuery(t); if (normalized) coreQueries.add(normalized); } });
    let queriesToProcess = Array.from(coreQueries).filter(q => q && q.length > 0);
    queriesToProcess = [...new Set(queriesToProcess)];
    if (queriesToProcess.length > WidgetConfig.MAX_TOTAL_TMDB_QUERIES_TO_PROCESS) {
        if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB搜索] 查询词过多 (${queriesToProcess.length}), 截断为 ${WidgetConfig.MAX_TOTAL_TMDB_QUERIES_TO_PROCESS} 个`);
        queriesToProcess = queriesToProcess.slice(0, WidgetConfig.MAX_TOTAL_TMDB_QUERIES_TO_PROCESS);
    }
    return queriesToProcess;
}

// --- TMDB搜索核心逻辑 ---
async function searchTmdb(originalTitle, chineseTitle, listTitle, searchMediaType = CONSTANTS.MEDIA_TYPES.TV, year = '') {
    const cacheKeyParams = { oT: originalTitle, cT: chineseTitle, lT: listTitle, media: searchMediaType, y: year };
    return CacheUtil.cachedOrFetch(CONSTANTS.CACHE_KEYS.TMDB_SEARCH, cacheKeyParams, async () => {
        let bestOverallMatch = null; let highestOverallScore = -Infinity;
        const validYear = year && /^\d{4}$/.test(year) ? parseInt(year, 10) : null;
        // 第一阶段: 严格年份搜索
        if (validYear && (originalTitle || chineseTitle)) {
            const preciseQueryText = normalizeTmdbQuery(originalTitle || chineseTitle);
            if (preciseQueryText) {
                if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB搜索] 阶段1: 查询 "${preciseQueryText}" (年份: ${validYear}, 类型: ${searchMediaType})`);
                try {
                    const params = { query: preciseQueryText, language: "zh-CN", include_adult: false };
                    if (searchMediaType === CONSTANTS.MEDIA_TYPES.TV) params.first_air_date_year = validYear; else params.primary_release_year = validYear;
                    const tmdbResponse = await fetchWithRetry(`/search/${searchMediaType}`, { params }, 'get', true, WidgetConfig.HTTP_MAIN_RETRIES);
                    const results = tmdbResponse?.results || (Array.isArray(tmdbResponse) ? tmdbResponse : null);
                    if (results?.length > 0) {
                        for (const result of results) {
                            if (searchMediaType === CONSTANTS.MEDIA_TYPES.TV && !(result.genre_ids && result.genre_ids.includes(CONSTANTS.TMDB_ANIMATION_GENRE_ID))) { if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB搜索] 阶段1: 跳过非动画TV结果 "${result.name || result.title}" (ID: ${result.id})`); continue; }
                            const resDate = result.release_date || result.first_air_date;
                            if (resDate && resDate.startsWith(String(validYear))) {
                                let score = scoreTmdbResult(result, preciseQueryText, validYear, searchMediaType, originalTitle, chineseTitle) + WidgetConfig.TMDB_SEARCH_STAGE1_YEAR_STRICT_SCORE_BOOST;
                                if (score > highestOverallScore) { highestOverallScore = score; bestOverallMatch = result; }
                            }
                        }
                        if (bestOverallMatch && WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB搜索] 阶段1 最佳匹配: ID ${bestOverallMatch.id}, 标题:"${bestOverallMatch.title || bestOverallMatch.name}", 分数: ${highestOverallScore.toFixed(2)}`);
                        if (highestOverallScore >= WidgetConfig.TMDB_SEARCH_STAGE1_HIGH_CONFIDENCE_EXIT_SCORE) { if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB搜索] 阶段1 高分退出 (${highestOverallScore.toFixed(2)} >= ${WidgetConfig.TMDB_SEARCH_STAGE1_HIGH_CONFIDENCE_EXIT_SCORE})`); return bestOverallMatch; }
                    }
                } catch (e) { if (WidgetConfig.DEBUG_LOGGING) console.error(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB搜索] 阶段1 API调用错误:`, e.message); if (String(e.message).includes("401") || String(e.message).includes("403")) throw e; }
            }
        }
        // 第二阶段: 宽泛搜索
        const queriesToProcess = generateTmdbSearchQueries(originalTitle, chineseTitle, listTitle);
        if (queriesToProcess.length === 0) { if (WidgetConfig.DEBUG_LOGGING && !bestOverallMatch) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB搜索] 阶段2: 无有效查询词且阶段1无匹配。`); return bestOverallMatch; }
        if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB搜索] 阶段2: 查询词 (${queriesToProcess.length}): ${JSON.stringify(queriesToProcess).substring(0,150)}...`);
        const queryPromises = queriesToProcess.map(query => async () => {
            try {
                const params = { query: query, language: "zh-CN", include_adult: false };
                const tmdbSearchResponse = await fetchWithRetry(`/search/${searchMediaType}`, { params }, 'get', true, WidgetConfig.HTTP_MAIN_RETRIES);
                const searchResults = tmdbSearchResponse?.results || (Array.isArray(tmdbSearchResponse) ? tmdbSearchResponse : null);
                let currentBestForQuery = null; let highScoreForQuery = -Infinity;
                if (searchResults?.length > 0) {
                    for (const result of searchResults) {
                        if (searchMediaType === CONSTANTS.MEDIA_TYPES.TV && !(result.genre_ids && result.genre_ids.includes(CONSTANTS.TMDB_ANIMATION_GENRE_ID))) { if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB搜索] 阶段2: 跳过非动画TV结果 "${result.name || result.title}" (ID: ${result.id}) for query "${query}"`); continue; }
                        const score = scoreTmdbResult(result, query, validYear, searchMediaType, originalTitle, chineseTitle);
                        if (score > highScoreForQuery) { highScoreForQuery = score; currentBestForQuery = result; }
                    }
                } return { result: currentBestForQuery, score: highScoreForQuery, query };
            } catch (e) { if (WidgetConfig.DEBUG_LOGGING) console.error(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB搜索] 阶段2 API调用错误，查询 "${query}":`, e.message); if (String(e.message).includes("401")||String(e.message).includes("403")) throw e; return { result: null, score: -Infinity, query }; }
        });
        for (let i = 0; i < queryPromises.length; i += WidgetConfig.MAX_CONCURRENT_TMDB_SEARCHES) {
            const batch = queryPromises.slice(i, i + WidgetConfig.MAX_CONCURRENT_TMDB_SEARCHES).map(p => p());
            try {
                const settledResults = await Promise.allSettled(batch);
                for (const sr of settledResults) {
                    if (sr.status === 'fulfilled' && sr.value.result && sr.value.score > highestOverallScore) { highestOverallScore = sr.value.score; bestOverallMatch = sr.value.result; if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB搜索] 阶段2 新总最佳 (来自查询 "${sr.value.query.substring(0,30)}...") ID ${bestOverallMatch.id}, 分数: ${highestOverallScore.toFixed(2)}`); }
                    else if (sr.status === 'rejected') { if (WidgetConfig.DEBUG_LOGGING) console.error(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB搜索] 阶段2 一个查询Promise被拒绝:`, sr.reason?.message); if (String(sr.reason?.message).includes("401")||String(sr.reason?.message).includes("403")) return null;}
                }
            } catch (batchError) { if (WidgetConfig.DEBUG_LOGGING) console.error(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB搜索] 阶段2 批处理执行错误:`, batchError.message); if (String(batchError.message).includes("401")||String(batchError.message).includes("403")) return null; }
        }
        if (bestOverallMatch && highestOverallScore >= WidgetConfig.TMDB_SEARCH_MIN_SCORE_THRESHOLD) { if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB搜索] 最终匹配: ID ${bestOverallMatch.id}, 标题:"${bestOverallMatch.title || bestOverallMatch.name}", 分数: ${highestOverallScore.toFixed(2)}`); return bestOverallMatch; }
        if (WidgetConfig.DEBUG_LOGGING) { const searchTarget = `BGM原名:"${originalTitle||''}" / 中文名:"${chineseTitle||''}"`; console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB搜索] 未找到满意的TMDB匹配项 (${searchTarget.substring(0,100)}...)。最高得分:${highestOverallScore.toFixed(2)} (阈值:${WidgetConfig.TMDB_SEARCH_MIN_SCORE_THRESHOLD})`);}
        return null; 
    });
}

// --- Bangumi列表项解析 ---
function parseBangumiListItems(htmlContent) {
    const $ = Widget.html.load(htmlContent);
    const pendingItems = [];
    $('ul#browserItemList li.item').each((index, element) => {
        const $item = $(element); let subjectId = $item.attr('id');
        if (subjectId && subjectId.startsWith('item_')) { subjectId = subjectId.substring(5); } else { if(WidgetConfig.DEBUG_LOGGING) console.warn(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM列表解析] 无法解析条目ID:`, $item.find('h3 a.l').text() || '未知条目'); return; }
        const titleElement = $item.find('div.inner > h3 > a.l'); const title = titleElement.text().trim(); const detailLink = titleElement.attr('href');
        if (!detailLink || !detailLink.trim()) { if(WidgetConfig.DEBUG_LOGGING) console.warn(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM列表解析] 条目 "${title}" (ID: ${subjectId}) 没有有效的详情链接，已跳过。`); return; }
        const fullDetailLink = `${WidgetConfig.BGM_BASE_URL}${detailLink}`; let listCoverUrl = $item.find('a.subjectCover img.cover').attr('src');
        if (listCoverUrl && listCoverUrl.startsWith('//')) { listCoverUrl = 'https:' + listCoverUrl; } else if (!listCoverUrl) { listCoverUrl = ''; }
        const rating = $item.find('div.inner > p.rateInfo > small.fade').text().trim(); const infoTextFromList = $item.find('div.inner > p.info.tip').text().trim();
        pendingItems.push({ id: subjectId, titleFromList: title, detailLink: fullDetailLink, coverFromList: listCoverUrl, ratingFromList: rating || "0", infoTextFromList: infoTextFromList });
    });
    if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM列表解析] 从列表页解析到 ${pendingItems.length} 个潜在条目。`);
    return pendingItems;
}

// --- 条目构建与数据集成 ---
function buildBaseItemStructure(pendingItem, detailData) {
    const {oTitle, cTitle, bPoster, rDate, dMTWidget, fRating} = detailData;
    const displayTitle = cTitle || oTitle || pendingItem.titleFromList || pendingItem.name_cn || pendingItem.name;
    let initialPoster = bPoster || pendingItem.coverFromList;
    if (!initialPoster && pendingItem.images?.large) {
        initialPoster = pendingItem.images.large.startsWith("//") ? "https:" + pendingItem.images.large : pendingItem.images.large;
    }
    return {
        id:String(pendingItem.id),
        type:"link",
        title:displayTitle,
        posterPath: initialPoster,
        backdropPath:'',
        releaseDate:rDate || pendingItem.air_date || '',
        mediaType:dMTWidget,
        rating:fRating || pendingItem.ratingFromList || (pendingItem.rating?.score ? pendingItem.rating.score.toFixed(1) : "0"),
        description: pendingItem.summary ? `[${pendingItem.weekday_cn || ''}] ${pendingItem.summary}`.trim() : (pendingItem.infoTextFromList || ""),
        genreTitle:null,
        link:pendingItem.detailLink || pendingItem.url || `${WidgetConfig.BGM_BASE_URL}/subject/${pendingItem.id}`,
        tmdb_id:null,
        tmdb_overview:"",
        tmdb_genres:null,
        tmdb_tagline:"",
        tmdb_status:"",
        tmdb_original_title:"",
        tmdb_preferred_title:"",
        tmdb_origin_countries: [],
        tmdb_vote_count: null,
        bgm_id: String(pendingItem.id),
        bgm_score: pendingItem.rating?.score || (parseFloat(pendingItem.ratingFromList) || 0),
        bgm_rating_total: pendingItem.rating?.total || 0,
        bgm_info_text: pendingItem.infoTextFromList || ""
    };
}

let tmdbFullDetailFetchQueue = [];
let isTmdbFullDetailFetchRunning = false;
async function processTmdbFullDetailQueue() {
    if (isTmdbFullDetailFetchRunning || tmdbFullDetailFetchQueue.length === 0) return;
    isTmdbFullDetailFetchRunning = true;
    const batchSize = WidgetConfig.MAX_CONCURRENT_TMDB_FULL_DETAILS_FETCH;
    const currentBatch = tmdbFullDetailFetchQueue.splice(0, batchSize);

    if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB完整详情队列] 处理批次大小: ${currentBatch.length}`);
    const promises = currentBatch.map(async (task) => {
        const { itemRef, tmdbSearchType, tmdbId } = task;
        try {
            if (!WidgetConfig.FETCH_FULL_TMDB_DETAILS) {
                if (WidgetConfig.DEBUG_LOGGING) console.warn(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB完整详情队列] 警告: TMDB ID ${tmdbId} 的任务在队列中，但 FETCH_FULL_TMDB_DETAILS 为 false，已跳过。`);
                return;
            }
            if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB完整详情队列] 正在获取/填充 TMDB ID ${tmdbId} 的详情 (类型: ${tmdbSearchType})`);
            
            const tmdbDetail = await CacheUtil.cachedOrFetch(
                CONSTANTS.CACHE_KEYS.TMDB_FULL_DETAIL,
                { type: tmdbSearchType, id: tmdbId },
                async () => {
                    const response = await fetchWithRetry(
                        `/${tmdbSearchType}/${tmdbId}`,
                        { params: { language: "zh-CN", append_to_response: WidgetConfig.TMDB_APPEND_TO_RESPONSE } },
                        'get', true, WidgetConfig.HTTP_MAIN_RETRIES
                    );
                    return response?.data || response;
                },
                { ttl: WidgetConfig.TTL_TMDB_FULL_DETAIL_MS }
            );
            if (tmdbDetail) {
                populateItemFromTmdbFullDetail(itemRef, tmdbDetail);
            } else if (WidgetConfig.DEBUG_LOGGING) {
                console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB完整详情队列] TMDB ID ${tmdbId} 从API/缓存未返回详情对象。`);
            }
        } catch (e) { if (WidgetConfig.DEBUG_LOGGING) console.warn(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB完整详情队列] 处理 TMDB ID ${tmdbId} 失败:`, e.message, e.stack?.substring(0,100)); }
    });
    await Promise.allSettled(promises);
    isTmdbFullDetailFetchRunning = false;
    if (tmdbFullDetailFetchQueue.length > 0) Promise.resolve().then(processTmdbFullDetailQueue);
    else if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB完整详情队列] 处理完毕。`);
}

async function integrateTmdbDataToItem(baseItem, tmdbResult, tmdbSearchType) {
    if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB集成] BGM "${(baseItem.title).substring(0,30)}..." -> TMDB "${(tmdbResult.title||tmdbResult.name||'').substring(0,30)}..." (ID:${tmdbResult.id})`);
    const originalBgmRating = baseItem.rating;
    const originalBgmReleaseDate = baseItem.releaseDate;
    const originalBgmPoster = baseItem.posterPath;

    baseItem.id = String(tmdbResult.id);
    baseItem.type = "tmdb";
    baseItem.mediaType = tmdbSearchType;
    baseItem.tmdb_id = String(tmdbResult.id);
    baseItem.title = (tmdbResult.title || tmdbResult.name || baseItem.title).trim();
    baseItem.posterPath = tmdbResult.poster_path || originalBgmPoster;
    baseItem.backdropPath = tmdbResult.backdrop_path || '';
    baseItem.releaseDate = parseDate(tmdbResult.release_date || tmdbResult.first_air_date) || originalBgmReleaseDate;
    baseItem.rating = tmdbResult.vote_average ? tmdbResult.vote_average.toFixed(1) : originalBgmRating;
    baseItem.description = tmdbResult.overview || baseItem.description;
    baseItem.genreTitle = null;
    baseItem.link = null;
    baseItem.tmdb_origin_countries = tmdbResult.origin_country || [];
    baseItem.tmdb_vote_count = tmdbResult.vote_count;

    if (WidgetConfig.FETCH_FULL_TMDB_DETAILS) {
        if (!tmdbFullDetailFetchQueue.some(task => task.tmdbId === tmdbResult.id && task.itemRef === baseItem)) {
             tmdbFullDetailFetchQueue.push({ itemRef: baseItem, tmdbSearchType, tmdbId: tmdbResult.id });
            if (!isTmdbFullDetailFetchRunning) Promise.resolve().then(processTmdbFullDetailQueue);
        } else if (WidgetConfig.DEBUG_LOGGING) {
            console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [TMDB集成] 条目 TMDB ID ${tmdbResult.id} (引用匹配) 已在完整详情队列中。`);
        }
    } else {
        const currentDescription = String(baseItem.description || "");
        const dayPrefixMatch = currentDescription.match(/^\[.*?\]\s*/);
        const dayPrefix = dayPrefixMatch ? dayPrefixMatch[0] : "";
        const baseDesc = currentDescription.replace(/^\[.*?\]\s*/, '');
        baseItem.description = `${dayPrefix}${baseDesc || ""}`.trim();
    }
}

async function getBangumiDetailCover(subjectId, subjectDetailUrl) {
    const cacheKeyParams = { subjectId };
    return CacheUtil.cachedOrFetch(CONSTANTS.CACHE_KEYS.BGM_DETAIL_COVER, cacheKeyParams, async () => {
        if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM详情封面] 尝试获取 BGM ID ${subjectId} 的高清封面从 ${subjectDetailUrl}`);
        try {
            const detailHtmlResponse = await fetchWithRetry(
                subjectDetailUrl,
                { headers: { "User-Agent": WidgetConfig.BGM_API_USER_AGENT, "Referer": `${WidgetConfig.BGM_BASE_URL}/`, "Accept-Language": "zh-CN,zh;q=0.9" } },
                'get', false, WidgetConfig.HTTP_RETRIES
            );
            if (!detailHtmlResponse?.data) return null;
            const $ = Widget.html.load(detailHtmlResponse.data);
            let bPoster = $('#bangumiInfo .infobox a.thickbox.cover[href*="/l/"]').attr('href') ||
                          $('#bangumiInfo .infobox a.thickbox.cover[href*="/g/"]').attr('href') ||
                          $('#bangumiInfo .infobox img.cover[src*="/l/"]').attr('src') ||
                          $('#bangumiInfo .infobox img.cover[src*="/g/"]').attr('src');
            if (!bPoster) {
                bPoster = $('#bangumiInfo .infobox a.thickbox.cover').attr('href') || $('#bangumiInfo .infobox img.cover').attr('src') || '';
            }
            if (bPoster.startsWith('//')) bPoster = 'https:' + bPoster;
            if (bPoster && bPoster.includes('lain.bgm.tv/pic/cover/')) {
                bPoster = bPoster.replace(/\/(m|c|g|s)\//, '/l/');
            }
            if (bPoster && !bPoster.startsWith('http') && bPoster.includes('lain.bgm.tv')) {
                bPoster = (bPoster.startsWith('/') ? 'https:' : 'https:') + bPoster;
            } else if (bPoster && !bPoster.startsWith('http')) {
                if (WidgetConfig.DEBUG_LOGGING) console.warn(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM详情封面] BGM ID ${subjectId} 的封面路径非预期相对路径: ${bPoster}`);
                return null;
            }
            return bPoster || null;
        } catch (e) { if (WidgetConfig.DEBUG_LOGGING) console.warn(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM详情封面] 获取 BGM ID ${subjectId} 封面失败: ${e.message}`); return null; }
    }, { ttl: WidgetConfig.TTL_BGM_DETAIL_COVER_MS });
}

// --- fetchItemDetails (极限速度优化后的版本) ---
async function fetchItemDetails(pendingItem, categoryHint) {
    if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM详情_极限] 处理 BGM ID: ${pendingItem.id} ("${pendingItem.titleFromList.substring(0,30)}...") 分类: ${categoryHint}`);

    let oTitle = pendingItem.titleFromList;
    let bPoster = pendingItem.coverFromList;
    let rDate = '';
    let fRating = pendingItem.ratingFromList || "0";
    let yearForTmdb = '';
    let dMTWidget = categoryHint;
    let tmdbSType = '';

    // 从列表页信息中解析年份
    if (pendingItem.infoTextFromList) {
        const yearMatch = pendingItem.infoTextFromList.match(/(\d{4})(?:年)?/);
        if (yearMatch && yearMatch[1]) {
            yearForTmdb = yearMatch[1];
            const dateMatchInInfo = pendingItem.infoTextFromList.match(/(\d{4}年\d{1,2}月\d{1,2}日|\d{4}年\d{1,2}月|\d{4}年[春夏秋冬]|\d{4}年)/);
            if (dateMatchInInfo?.[0]) rDate = parseDate(dateMatchInInfo[0]);
        }
    }
    if (!rDate && yearForTmdb) rDate = `${yearForTmdb}-01-01`;

    const isTmdbRelevantCategory = categoryHint === CONSTANTS.MEDIA_TYPES.ANIME || categoryHint === CONSTANTS.MEDIA_TYPES.REAL;

    if (isTmdbRelevantCategory) {
        // 从列表数据中猜测TMDB搜索类型 (tv/movie)
        dMTWidget = CONSTANTS.MEDIA_TYPES.TV;
        const titleLower = pendingItem.titleFromList.toLowerCase();
        const infoLower = (pendingItem.infoTextFromList || "").toLowerCase();
        if (titleLower.includes("movie") || titleLower.includes("剧场版") || titleLower.includes("映画") ||
            infoLower.includes("movie") || infoLower.includes("剧场版") || infoLower.includes("映画")) {
            dMTWidget = CONSTANTS.MEDIA_TYPES.MOVIE;
        }
        tmdbSType = dMTWidget;
    }

    const item = buildBaseItemStructure(pendingItem, { oTitle, cTitle: '', bPoster, rDate, dMTWidget, fRating });
    
    if (isTmdbRelevantCategory && tmdbSType) {
        if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM详情_极限] TMDB搜索 (仅列表数据) for BGM ID ${pendingItem.id}: 标题="${oTitle}", 年份="${yearForTmdb}", 类型="${tmdbSType}"`);
        const tmdbRes = await searchTmdb(oTitle, null, oTitle, tmdbSType, yearForTmdb);
        if (tmdbRes?.id) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM详情_极限] TMDB匹配成功 (来自列表数据) for BGM ID ${pendingItem.id}. TMDB ID: ${tmdbRes.id}`);
            await integrateTmdbDataToItem(item, tmdbRes, tmdbSType);
        } else {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM详情_极限] TMDB匹配失败 (来自列表数据) for BGM ID ${pendingItem.id}. 将使用 BGM 列表数据。`);
            // 此路径下，不获取 BGM 详情页 HTML，只保留列表数据。
        }
    } else {
        // 非影视相关分类（书籍、游戏、音乐）或 TMDB 类型未确定时，仍获取 BGM HTML 详情页
        if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM详情_极限] 非TMDB相关分类 (${categoryHint})。获取 BGM HTML 详情页 for BGM ID ${pendingItem.id}`);
        try {
            const detailHtmlResponse = await fetchWithRetry( pendingItem.detailLink, { headers: { "User-Agent": WidgetConfig.BGM_API_USER_AGENT, "Referer": `${WidgetConfig.BGM_BASE_URL}/`, "Accept-Language": "zh-CN,zh;q=0.9" } }, 'get', false, WidgetConfig.HTTP_MAIN_RETRIES );
            if (!detailHtmlResponse?.data) throw new Error(`Bangumi详情页数据为空或无效: ${pendingItem.detailLink}`);
            
            const $ = Widget.html.load(detailHtmlResponse.data);
            item.title = ($('h1.nameSingle > a').first().text().trim()) || item.title;
            const cnTitleFromDetail = getInfoFromBox($, "中文名:");
            if (cnTitleFromDetail) item.title = cnTitleFromDetail;

            let detailPagePoster = $('#bangumiInfo .infobox a.thickbox.cover[href*="/l/"]').attr('href') || $('#bangumiInfo .infobox img.cover[src*="/l/"]').attr('src') || $('#bangumiInfo .infobox a.thickbox.cover').attr('href') || $('#bangumiInfo .infobox img.cover').attr('src');
            if (detailPagePoster) {
                if (detailPagePoster.startsWith('//')) detailPagePoster = 'https:' + detailPagePoster;
                if (detailPagePoster.includes('lain.bgm.tv/pic/cover/')) detailPagePoster = detailPagePoster.replace(/\/(m|c|g|s)\//, '/l/');
                item.posterPath = detailPagePoster;
            }
            let rDateStrFromDetail = getInfoFromBox($, "放送开始:") || getInfoFromBox($, "上映年度:") || getInfoFromBox($, "发售日期:") || getInfoFromBox($, "发行日期:");
            item.releaseDate = parseDate(rDateStrFromDetail) || item.releaseDate;
            item.rating = ($('#panelInterestWrapper .global_rating .number').text().trim()) || item.rating;
            item.description = getInfoFromBox($, "简介") || item.description;
            
        } catch (htmlError) {
            console.error(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM详情_极限] 获取/解析非TMDB相关分类的BGM HTML失败 (ID ${pendingItem.id}):`, htmlError.message);
        }
    }

    if (WidgetConfig.DEBUG_LOGGING) {
        const logItemOutput = {...item};
        if(logItemOutput.tmdb_overview?.length>30) logItemOutput.tmdb_overview = logItemOutput.tmdb_overview.substring(0,27)+"...";
        if(logItemOutput.description?.length>30) logItemOutput.description = logItemOutput.description.substring(0,27)+"...";
        console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM详情_极限] 处理完成: BGM_ID:${pendingItem.id}, 最终ID:${logItemOutput.id}, 类型:${logItemOutput.type}, 标题:"${logItemOutput.title.substring(0,30)}"...`);
    }
    return item;
}

async function processBangumiPage(url, categoryHint, currentPageString, rankingContextInfo = {}) {
    const currentPage = currentPageString ? parseInt(currentPageString, 10) : 0;
    if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM页面处理] 列表页: ${url} (当前页: ${currentPage > 0 ? currentPage : '未知/1'})`);
    let listHtml;
    const commonHeaders = { "User-Agent": WidgetConfig.BGM_API_USER_AGENT, "Referer": `${WidgetConfig.BGM_BASE_URL}/`, "Accept-Language": "zh-CN,zh;q=0.9" };
    const prefetchedHtmlPromise = PrefetchCache.get(url);
    if (prefetchedHtmlPromise) { if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM页面处理] 使用预取缓存中的HTML Promise: ${url}`); try { listHtml = await prefetchedHtmlPromise; } catch (e) { if (WidgetConfig.DEBUG_LOGGING) console.warn(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM页面处理] 预取HTML的Promise解析失败 (${url}): ${e.message}。将尝试重新获取。`); listHtml = null; } }
    if (!listHtml) { if (WidgetConfig.DEBUG_LOGGING && !prefetchedHtmlPromise) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM页面处理] 未在预取缓存中找到或预取失败，正常获取HTML: ${url}`); try { const listHtmlResp = await fetchWithRetry(url, { headers: commonHeaders }, 'get', false, WidgetConfig.HTTP_MAIN_RETRIES); if (!listHtmlResp?.data) throw new Error("列表页响应数据为空或无效"); listHtml = listHtmlResp.data; } catch (e) { console.error(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM页面处理] 获取列表页 ${url} 失败:`, e.message); throw new Error(`请求Bangumi列表页失败: ${e.message}`); } }
    if (currentPage > 0) { const nextPageNum = currentPage + 1; let nextPageUrl; if (url.includes("page=")) { nextPageUrl = url.replace(/page=\d+/, `page=${nextPageNum}`); } else if (url.includes("?")) { nextPageUrl = `${url}&page=${nextPageNum}`; } else { nextPageUrl = `${url}?page=${nextPageNum}`; } if (nextPageUrl && nextPageUrl !== url) { if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM页面处理] 触发下一页 (${nextPageNum}) 的HTML预取: ${nextPageUrl}`); PrefetchCache.fetchAndCacheHtml(nextPageUrl, commonHeaders).catch(()=>{}); } }
    const pendingItems = parseBangumiListItems(listHtml);
    if (pendingItems.length === 0) { if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM页面处理] 从HTML未解析到任何条目。`); return []; }
    if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM页面处理] 解析到 ${pendingItems.length} 个条目。开始并发获取详情 (最大并发: ${WidgetConfig.MAX_CONCURRENT_DETAILS_FETCH})...`);
    const results = [];
    for (let i = 0; i < pendingItems.length; i += WidgetConfig.MAX_CONCURRENT_DETAILS_FETCH) {
        const batch = pendingItems.slice(i, i + WidgetConfig.MAX_CONCURRENT_DETAILS_FETCH);
        if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM页面处理] 处理详情批次 ${Math.floor(i/WidgetConfig.MAX_CONCURRENT_DETAILS_FETCH)+1} (数量: ${batch.length})`);
        const detailPromises = batch.map(item => CacheUtil.cachedOrFetch( CONSTANTS.CACHE_KEYS.ITEM_DETAIL_COMPUTED, { itemId: item.id, category: categoryHint, scriptVer: WidgetMetadata.version }, () => fetchItemDetails(item, categoryHint), { calculateTTL: calculateContentTTL, context: { currentDate: new Date() }, ttlIdentifier: rankingContextInfo } ).catch(e => { console.error(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM页面处理] 条目详情处理失败 (BGM ID: ${item.id}): `, e.message); return null; }) );
        const settledResults = await Promise.allSettled(detailPromises);
        settledResults.forEach(sr => { if (sr.status === 'fulfilled' && sr.value) { results.push(sr.value); } else if (sr.status === 'rejected') { console.error(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM页面处理] 一个条目详情Promise被拒绝:`, sr.reason?.message); } });
    }
    if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [BGM页面处理] 列表页处理完成。返回 ${results.length} 条有效结果.`);
    return results;
}

async function fetchRecentHot(params = {}) {
    const category = params.category || CONSTANTS.MEDIA_TYPES.ANIME; const page = params.page || "1";
    const url = `${WidgetConfig.BGM_BASE_URL}/${category}/browser/?sort=trends&page=${page}`;
    if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [模式] 获取近期热门: 分类=${category}, 页=${page}`);
    try { return await processBangumiPage(url, category, page, { category, sort: 'trends' }); }
    catch (error) { console.error(`${CONSTANTS.LOG_PREFIX_GENERAL} [模式] fetchRecentHot(分类:${category}, 页码:${page}) 发生顶层错误:`, error.message); return []; }
}

async function fetchAirtimeRanking(params = {}) {
    const category = params.category || CONSTANTS.MEDIA_TYPES.ANIME;
    const year = params.year || "";
    const month = params.month || "all";
    const sort = params.sort || "rank";
    const page = params.page || "1";
    let url;

    if (WidgetConfig.DEBUG_LOGGING) {
        console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [fetchAirtimeRanking参数] 分类: ${category}, 年份: '${year}', 月份: '${month}', 排序: ${sort}, 页码: ${page}`);
    }

    if (year && year !== "" && /^\d{4}$/.test(year)) {
        let airtimePath = `airtime/${year}`;
        if (month && month !== "" && month !== "all" && /^\d{1,2}$/.test(month)) {
            airtimePath += `/${month}`;
        }
        url = `${WidgetConfig.BGM_BASE_URL}/${category}/browser/${airtimePath}?sort=${sort}&page=${page}`;
    } else {
        url = `${WidgetConfig.BGM_BASE_URL}/${category}/browser/?sort=${sort}&page=${page}`;
        if (year && year !== "" && WidgetConfig.DEBUG_LOGGING) {
            console.warn(`${CONSTANTS.LOG_PREFIX_GENERAL} [模式] 时段排行提供的年份 "${year}" 格式无效。将浏览所有年份。`);
        }
    }

    if (WidgetConfig.DEBUG_LOGGING) {
        console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [模式] 获取时段排行: URL=${url}`);
    }

    try {
        const rankingContextInfo = { category, year, month, sort };
        if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [fetchAirtimeRanking] rankingContextInfo:`, rankingContextInfo);
        return await processBangumiPage(url, category, page, rankingContextInfo);
    } catch (error) {
        console.error(`${CONSTANTS.LOG_PREFIX_GENERAL} [模式] fetchAirtimeRanking(分类:${category}, 年:${year}, 月:${month}, 排序:${sort}, 页:${page}) 发生顶层错误:`, error.message, error.stack);
        return [];
    }
}

// --- 每日放送后台增强函数 ---
async function enhanceCalendarItemInBackground(apiItemData, initialVideoItem) {
    if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [每日放送增强] 处理 BGM ID: ${apiItemData.id}, 初始标题: ${initialVideoItem.title.substring(0,30)}`);
    
    let tmdbResultForLogic = null;
    let itemChangedByTmdb = false;
    let finalPosterPath = initialVideoItem.posterPath;
    let tmdbSearchType = '';

    try {
        const { id: bgmId, name: bgmName, name_cn: bgmNameCn, air_date: bgmAirDate, type: bgmApiType, url: bgmUrl } = apiItemData;
        const itemTitleForSearch = bgmNameCn || bgmName;
        const itemYear = bgmAirDate ? bgmAirDate.substring(0, 4) : '';
        
        const bgmCategoryHint = CONSTANTS.BGM_API_TYPE_MAPPING[bgmApiType] || 'unknown';

        if (bgmCategoryHint === CONSTANTS.MEDIA_TYPES.ANIME) tmdbSearchType = CONSTANTS.MEDIA_TYPES.TV;
        else if (bgmCategoryHint === CONSTANTS.MEDIA_TYPES.REAL) tmdbSearchType = CONSTANTS.MEDIA_TYPES.MOVIE;

        if (tmdbSearchType) {
            tmdbResultForLogic = await searchTmdb(bgmName, bgmNameCn, itemTitleForSearch, tmdbSearchType, itemYear);
        }

        if (tmdbResultForLogic?.id) {
            itemChangedByTmdb = true;
            if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [每日放送增强] TMDB 匹配成功 for BGM ID ${bgmId}: TMDB ID ${tmdbResultForLogic.id}, 类型: ${tmdbSearchType}`);
            
            initialVideoItem.id = String(tmdbResultForLogic.id);
            initialVideoItem.type = "tmdb";
            initialVideoItem.mediaType = tmdbSearchType;
            initialVideoItem.tmdb_id = String(tmdbResultForLogic.id);
            initialVideoItem.title = (tmdbResultForLogic.title || tmdbResultForLogic.name || itemTitleForSearch).trim();
            finalPosterPath = tmdbResultForLogic.poster_path || finalPosterPath;
            initialVideoItem.backdropPath = tmdbResultForLogic.backdrop_path || '';
            initialVideoItem.releaseDate = parseDate(tmdbResultForLogic.release_date || tmdbResultForLogic.first_air_date) || bgmAirDate;
            initialVideoItem.rating = tmdbResultForLogic.vote_average ? tmdbResultForLogic.vote_average.toFixed(1) : initialVideoItem.rating;
            initialVideoItem.link = null;

            // 立即尝试获取基础详情以获得国家信息和投票数
            try {
                const basicTmdbDetailResponse = await fetchWithRetry(`/${tmdbSearchType}/${tmdbResultForLogic.id}`, { params: { language: "zh-CN" } }, 'get', true, 1);
                const basicTmdbDetail = basicTmdbDetailResponse?.data || basicTmdbDetailResponse;
                if (basicTmdbDetail) {
                    if (basicTmdbDetail.origin_country && Array.isArray(basicTmdbDetail.origin_country) && basicTmdbDetail.origin_country.length > 0) {
                        initialVideoItem.tmdb_origin_countries = basicTmdbDetail.origin_country;
                    } else if (basicTmdbDetail.production_countries && Array.isArray(basicTmdbDetail.production_countries) && basicTmdbDetail.production_countries.length > 0) {
                        initialVideoItem.tmdb_origin_countries = basicTmdbDetail.production_countries.map(pc => pc.iso_3166_1);
                    } else {
                        initialVideoItem.tmdb_origin_countries = [];
                    }
                    if (typeof basicTmdbDetail.vote_count === 'number') {
                        initialVideoItem.tmdb_vote_count = basicTmdbDetail.vote_count;
                    }
                     if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [每日放送增强] TMDB ID ${tmdbResultForLogic.id} 初步国家/投票数信息:`, initialVideoItem.tmdb_origin_countries, initialVideoItem.tmdb_vote_count);
                }
            } catch (e) {
                if (WidgetConfig.DEBUG_LOGGING) console.warn(`${CONSTANTS.LOG_PREFIX_GENERAL} [每日放送增强] 获取TMDB ID ${tmdbResultForLogic.id} 初步国家/投票数信息失败: ${e.message}`);
            }

            if (WidgetConfig.FETCH_FULL_TMDB_DETAILS) {
                tmdbFullDetailFetchQueue.push({itemRef: initialVideoItem, tmdbSearchType, tmdbId: tmdbResultForLogic.id });
                if(!isTmdbFullDetailFetchRunning) Promise.resolve().then(processTmdbFullDetailQueue);
            }
        } else {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [每日放送增强] TMDB 未匹配 BGM ID ${bgmId}.`);
            initialVideoItem.bgm_score = apiItemData.rating?.score || 0;
            initialVideoItem.bgm_rating_total = apiItemData.rating?.total || 0;
        }

        // 封面处理：如果未被TMDB匹配覆盖，且当前为BGM API的默认小图，则尝试获取BGM详情页高清图
        const bgmApiPoster = (apiItemData.images?.large || apiItemData.images?.common || "").startsWith("//") ? "https:" + (apiItemData.images?.large || apiItemData.images?.common) : (apiItemData.images?.large || apiItemData.images?.common || "");
        if (finalPosterPath === bgmApiPoster && bgmApiPoster !== "" && !tmdbResultForLogic?.poster_path) {
            const bgmDetailUrl = bgmUrl || `${WidgetConfig.BGM_BASE_URL}/subject/${bgmId}`;
             if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [每日放送增强] 封面仍是BGM API图，尝试获取BGM详情页高清封面 for ${bgmDetailUrl}`);
            try {
                const bgmHighResCover = await getBangumiDetailCover(String(bgmId), bgmDetailUrl);
                if (bgmHighResCover) {
                    if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [每日放送增强] 使用 BGM 高清封面。`);
                    finalPosterPath = bgmHighResCover;
                }
            } catch (coverError) {
                 if (WidgetConfig.DEBUG_LOGGING) console.warn(`${CONSTANTS.LOG_PREFIX_GENERAL} [每日放送增强] 获取 BGM 高清封面失败: ${coverError.message}`);
            }
        }
        initialVideoItem.posterPath = finalPosterPath;

        CacheUtil.set(CONSTANTS.CACHE_KEYS.CALENDAR_ITEM_FINAL_DISPLAY, String(bgmId), Promise.resolve({...initialVideoItem}), WidgetConfig.TTL_CALENDAR_ITEM_ENHANCED_MS);
        
        if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [每日放送增强] 处理完成 BGM ID: ${bgmId}. 最终ID: ${initialVideoItem.id}, 类型: ${initialVideoItem.type}`);
        return initialVideoItem;

    } catch (error) {
        console.error(`${CONSTANTS.LOG_PREFIX_GENERAL} [每日放送增强] 处理 BGM ID ${apiItemData.id} 时发生错误:`, error.message, error.stack);
        initialVideoItem.bgm_score = apiItemData.rating?.score || 0;
        initialVideoItem.bgm_rating_total = apiItemData.rating?.total || 0;
        return initialVideoItem;
    }
}

// --- 每日放送主函数 ---
async function fetchDailyCalendarApi(params = {}) {
    const filterType = params.filterType || "today";
    const specificWeekdayParam = (filterType === "specific_day") ? params.specificWeekday : null;
    const sortOrder = params.dailySortOrder || "popularity_rat_bgm";
    const regionFilter = params.dailyRegionFilter || "all";

    if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [每日放送] 参数 - 筛选: ${filterType}, 星期: ${specificWeekdayParam}, 排序: ${sortOrder}, 区域: ${regionFilter}`);
    
    const actualApiUrl = `https://api.bgm.tv/calendar`;
    try {
        const apiResponse = await CacheUtil.cachedOrFetch(
            CONSTANTS.CACHE_KEYS.BGM_CALENDAR_API,
            'weekly_broadcast_data_v1.2',
            async () => {
                const response = await Widget.http.get(actualApiUrl, { headers: { "User-Agent": WidgetConfig.BGM_API_USER_AGENT } });
                if (!response || !response.data) throw new Error("Bangumi 日历 API 响应为空或无效");
                if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [每日放送] 日历 API 原始响应 (前500字符):`, JSON.stringify(response.data).substring(0, 500));
                return response.data;
            },
            { ttl: WidgetConfig.TTL_CALENDAR_API_MS }
        );

        if (!Array.isArray(apiResponse)) {
            console.error(`${CONSTANTS.LOG_PREFIX_GENERAL} [每日放送 API] 响应数据格式不正确。`); return [];
        }

        let filteredApiItems = [];
        const today = new Date();
        const currentJsDay = today.getDay();
        const bgmApiTodayId = CONSTANTS.JS_DAY_TO_BGM_API_ID[currentJsDay];
        let targetBgmApiWeekdayId = null;

        if (filterType === "today") {
            targetBgmApiWeekdayId = bgmApiTodayId;
        } else if (filterType === "specific_day") {
            targetBgmApiWeekdayId = specificWeekdayParam ? parseInt(specificWeekdayParam, 10) : -1;
            if (isNaN(targetBgmApiWeekdayId) || targetBgmApiWeekdayId < 1 || targetBgmApiWeekdayId > 7) {
                if (WidgetConfig.DEBUG_LOGGING) console.warn(`${CONSTANTS.LOG_PREFIX_GENERAL} [每日放送] 无效的指定星期: ${specificWeekdayParam}，返回空列表。`);
                return [];
            }
        }

        apiResponse.forEach(dayData => {
            if (!dayData.items || !Array.isArray(dayData.items)) return;
            const dayOfWeekId = dayData.weekday?.id;
            let includeDay = false;
            switch (filterType) {
                case "today": if (dayOfWeekId === targetBgmApiWeekdayId) includeDay = true; break;
                case "specific_day":
                    if (targetBgmApiWeekdayId !== -1 && dayOfWeekId === targetDayOfWeekId) includeDay = true;
                    break;
                case "mon_thu": if (dayOfWeekId >= 1 && dayOfWeekId <= 4) includeDay = true; break;
                case "fri_sun": if (dayOfWeekId >= 5 && dayOfWeekId <= 7) includeDay = true; break;
                case "all_week": default: includeDay = true; break;
            }
            if (includeDay) {
                filteredApiItems.push(...dayData.items.map(item => ({ ...item, weekday_cn: dayData.weekday?.cn || `周${dayOfWeekId}` })));
            }
        });
        
        if (filteredApiItems.length === 0 && WidgetConfig.DEBUG_LOGGING) {
            console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [每日放送] 筛选后无任何条目 (筛选类型: ${filterType}, 目标BGM星期ID: ${targetBgmApiWeekdayId})`);
        }
        if (filteredApiItems.length === 0) return [];

        const resultsToReturn = [];
        const enhancementPromises = [];

        for (const item of filteredApiItems) {
            const bgmIdStr = String(item.id);
            const cachedFinalItem = await CacheUtil.get(CONSTANTS.CACHE_KEYS.CALENDAR_ITEM_FINAL_DISPLAY, bgmIdStr);
            if (cachedFinalItem) {
                cachedFinalItem.bgm_rating_total = item.rating?.total || cachedFinalItem.bgm_rating_total || 0;
                cachedFinalItem.bgm_score = item.rating?.score || cachedFinalItem.bgm_score || 0;
                cachedFinalItem.bgm_air_date = item.air_date || cachedFinalItem.bgm_air_date;
                const currentDescription = String(cachedFinalItem.description || "");
                if (!currentDescription.startsWith(`[${item.weekday_cn}]`)) {
                    const baseDesc = currentDescription.replace(/^\[.*?\]\s*/, '');
                    cachedFinalItem.description = `[${item.weekday_cn}] ${baseDesc}`.trim();
                }
                if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [每日放送] 使用缓存的增强条目 BGM ID: ${bgmIdStr}`);
                resultsToReturn.push(cachedFinalItem);
                continue;
            }

            let cover = item.images?.large || item.images?.common || item.images?.medium || item.images?.grid || item.images?.small || "";
            if (cover.startsWith("//")) cover = "https:" + cover;
            if (cover && cover.includes('lain.bgm.tv/pic/cover/')) {
                cover = cover.replace(/\/(m|c|g|s)\//, '/l/');
            }
            const mediaTypeNum = item.type;
            const mediaTypeStr = CONSTANTS.BGM_API_TYPE_MAPPING[mediaTypeNum] || "unknown";
            const dayName = item.weekday_cn || "放送日";

            const videoItem = {
                id: bgmIdStr, type: "link",
                title: `${item.name_cn || item.name}`,
                posterPath: cover, backdropPath: "",
                releaseDate: item.air_date || "",
                mediaType: mediaTypeStr,
                rating: item.rating?.score ? item.rating.score.toFixed(1) : "N/A",
                description: `[${dayName}] ${item.summary || ""}`.trim(),
                link: item.url || `${WidgetConfig.BGM_BASE_URL}/subject/${item.id}`,
                tmdb_id: null,
                bgm_id: String(item.id),
                bgm_collection_count: item.collection?.collect || 0,
                bgm_rating_total: item.rating?.total || 0,
                bgm_score: item.rating?.score || 0,
                bgm_air_date: item.air_date,
                tmdb_origin_countries: [],
                tmdb_vote_count: null,
                tmdb_overview: "",
                tmdb_genres: null,
                tmdb_tagline: "",
                tmdb_status: "",
                tmdb_original_title:"",
                tmdb_preferred_title:""
            };
            enhancementPromises.push(enhanceCalendarItemInBackground(item, videoItem));
            resultsToReturn.push(videoItem);
        }
        
        if (enhancementPromises.length > 0) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [每日放送] 等待 ${enhancementPromises.length} 个条目的核心增强...`);
            await Promise.allSettled(enhancementPromises);
            if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [每日放送] 核心增强完成。`);
        }
        
        // --- 排序逻辑 ---
        if (sortOrder !== "default") {
            resultsToReturn.sort((a, b) => {
                try {
                    if (sortOrder === "popularity_rat_bgm") {
                        return (b.bgm_rating_total || 0) - (a.bgm_rating_total || 0);
                    }
                    if (sortOrder === "score_bgm_desc") {
                        return (b.bgm_score || 0) - (a.bgm_score || 0);
                    }
                    if (sortOrder === "airdate_desc") {
                        const dateA = a.releaseDate || a.bgm_air_date;
                        const dateB = b.releaseDate || b.bgm_air_date;
                        const timeA = dateA ? new Date(dateA).getTime() : 0;
                        const timeB = dateB ? new Date(dateB).getTime() : 0;
                        if (!timeA && !timeB) return 0;
                        if (!timeA) return 1;
                        if (!timeB) return -1;
                        return timeB - timeA;
                    }
                } catch (e) { console.error(`${CONSTANTS.LOG_PREFIX_GENERAL} [每日放送排序错误]`, e); return 0; }
                return 0;
            });
        }

        // --- 区域筛选逻辑 ---
        let finalFilteredResults = resultsToReturn;
        if (regionFilter !== "all") {
            finalFilteredResults = resultsToReturn.filter(item => {
                if (item.type === "tmdb" && item.tmdb_id) {
                    const countries = item.tmdb_origin_countries || [];
                    if (WidgetConfig.DEBUG_LOGGING && countries.length > 0 && item.title) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [区域筛选] TMDB条目 "${item.title.substring(0,20)}..." 国家: ${countries.join(',')}`);
                    if (countries.length === 0) return regionFilter === "OTHER";
                    if (regionFilter === "JP") return countries.includes("JP");
                    if (regionFilter === "CN") return countries.includes("CN");
                    if (regionFilter === "US_EU") return countries.some(c => CONSTANTS.REGION_FILTER_US_EU_COUNTRIES.includes(c));
                    if (regionFilter === "OTHER") {
                        const isJPCNUSEU = countries.includes("JP") || countries.includes("CN") || countries.some(c => CONSTANTS.REGION_FILTER_US_EU_COUNTRIES.includes(c));
                        return !isJPCNUSEU;
                    }
                    return false;
                } else {
                    return regionFilter === "all" || regionFilter === "OTHER";
                }
            });
        }
        
        if (WidgetConfig.DEBUG_LOGGING) console.log(`${CONSTANTS.LOG_PREFIX_GENERAL} [每日放送 API] 最终处理完成，返回 ${finalFilteredResults.length} 个条目。`);
        return finalFilteredResults;

    } catch (error) {
        console.error(`${CONSTANTS.LOG_PREFIX_GENERAL} [模式] fetchDailyCalendarApi 发生错误:`, error.message, error.stack);
        return [];
    }
}
