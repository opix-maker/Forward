// --- 小部件配置 ---
const WidgetConfig = {
    MAX_CONCURRENT_DETAILS_FETCH: 6,
    MAX_CONCURRENT_TMDB_SEARCHES: 3, 
    MAX_CONCURRENT_CALENDAR_ENHANCE: 3, 
    MAX_CONCURRENT_TMDB_FULL_DETAILS_FETCH: 2, 
    HTTP_RETRIES: 1,
    HTTP_MAIN_RETRIES: 2,
    HTTP_RETRY_DELAY: 1000,
    FETCH_FULL_TMDB_DETAILS: true, 
    TMDB_APPEND_TO_RESPONSE: "translations,credits",
    TMDB_SEARCH_STAGE1_YEAR_STRICT_SCORE_BOOST: 12,
    TMDB_SEARCH_STAGE1_HIGH_CONFIDENCE_EXIT_SCORE: 23,
    TMDB_SEARCH_MIN_SCORE_THRESHOLD: 6, 
    CACHE_TTL_MS: 25 * 60 * 1000,
    PREFETCH_CACHE_TTL_MS: 2 * 60 * 1000,
    MAX_PREFETCHED_PAGES: 5,
    DEBUG_LOGGING: false, 
    BGM_BASE_URL: "https://bgm.tv",
    BGM_API_USER_AGENT: "", 
    TTL_TRENDS_MS: 6 * 60 * 60 * 1000,
    TTL_RANK_MS: 24 * 60 * 60 * 1000,
    TTL_SEASON_EARLY_MS: 12 * 60 * 60 * 1000,
    TTL_SEASON_LATE_MS: 3 * 24 * 60 * 60 * 1000,
    TTL_ARCHIVE_MS: 7 * 24 * 60 * 60 * 1000,
    TTL_CALENDAR_API_MS: 6 * 60 * 60 * 1000, 
    TTL_CALENDAR_ITEM_ENHANCED_MS: 24 * 60 * 60 * 1000,
    TTL_BGM_DETAIL_COVER_MS: 7 * 24 * 60 * 60 * 1000, 
    SEASON_EARLY_WEEKS: 6,
    TMDB_STAGE2_CORE_QUERY_THRESHOLD_BOOST: 5, 
    MAX_TOTAL_TMDB_QUERIES_TO_PROCESS: 10,    
    TMDB_ANIMATION_GENRE_ID: 16
};

var WidgetMetadata = {
    id: "bangumi_charts_tmdb_v3", 
    title: "Bangumi 热门榜单",
    description: "从Bangumi获取近期热门、每日放送数据，支持榜单筛选，智能匹配TMDB数据。",
    author: "Autism ",
    site: "https://github.com/opix-maker/Forward", 
    version: "1.0.2", 
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
                { name: "sort", title: "排序方式", type: "enumeration", value: "rank", enumOptions: [ { title: "排名", value: "rank" }, { title: "热度", value: "trends" }, { title: "收藏数", value: "collects" }, { title: "发售日期", value: "date" }, { title: "名称", value: "title" } ] },
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

// --- 缓存工具 ---
const CacheUtil = {
    cache: new Map(),
    pendingPromises: new Map(),
    _generateKey: function(type, identifier) {
        if (typeof identifier === 'object' && identifier !== null) {
            try { return `${type}_${JSON.stringify(Object.keys(identifier).sort().reduce((obj, key) => { obj[key] = identifier[key]; return obj; }, {}))}`; }
            catch (e) { return `${type}_${String(identifier)}`; }
        }
        return `${type}_${String(identifier)}`;
    },
    get: function(type, identifier) {
        const key = this._generateKey(type, identifier);
        if (this.pendingPromises.has(key)) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`[CacheUtil] 等待进行中: ${key.substring(0, 80)}...`);
            return this.pendingPromises.get(key);
        }
        const entry = this.cache.get(key);
        if (entry && Date.now() < entry.expiry) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`[CacheUtil] 命中缓存: ${key.substring(0, 80)}...`);
            return Promise.resolve(entry.value);
        } else if (entry) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`[CacheUtil] 缓存过期: ${key.substring(0, 80)}...`);
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
            if (WidgetConfig.DEBUG_LOGGING) console.log(`[CacheUtil] 设置缓存: ${key.substring(0, 80)}... (TTL: ${ttlToUse / 1000}s)`);
            this.cache.set(key, { value: value, expiry: Date.now() + ttlToUse });
            this.pendingPromises.delete(key);
            return value;
        }).catch(error => {
            if (WidgetConfig.DEBUG_LOGGING) console.warn(`[CacheUtil] Promise执行失败，从pending移除: ${key.substring(0, 80)}...`, error.message);
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
            if (WidgetConfig.DEBUG_LOGGING) console.log(`[预取缓存] 命中: ${url}`);
            return entry.promise; 
        }
        if (entry) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`[预取缓存] 过期或无效: ${url}`);
            this.prefetchedHtml.delete(url);
        }
        return null;
    },
    set: function(url, htmlPromise) {
        if (WidgetConfig.DEBUG_LOGGING) console.log(`[预取缓存] 开始预取并设置Promise: ${url}`);
        const entry = { promise: htmlPromise, timestamp: Date.now(), inProgress: true };
        this.prefetchedHtml.set(url, entry);

        htmlPromise.finally(() => { 
             const currentEntry = this.prefetchedHtml.get(url);
             if (currentEntry === entry) { 
                currentEntry.inProgress = false;
                htmlPromise.catch(() => {
                    if (this.prefetchedHtml.get(url) === entry) {
                        this.prefetchedHtml.delete(url);
                        if (WidgetConfig.DEBUG_LOGGING) console.log(`[预取缓存] 预取失败后删除条目: ${url}`);
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
                if (WidgetConfig.DEBUG_LOGGING) console.log(`[预取缓存] 清理最旧条目: ${oldestKey}`);
            }
        }
        return htmlPromise;
    },
    fetchAndCacheHtml: function(url, headers) {
        let existingEntry = this.prefetchedHtml.get(url);
        if (existingEntry && (existingEntry.inProgress || (Date.now() - existingEntry.timestamp < WidgetConfig.PREFETCH_CACHE_TTL_MS))) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`[预取缓存] 使用现有预取Promise: ${url}`);
            return existingEntry.promise;
        }
        if (existingEntry) { 
             this.prefetchedHtml.delete(url);
        }
        const newHtmlPromise = fetchWithRetry(url, { headers }, 'get', false, WidgetConfig.HTTP_RETRIES) 
            .then(response => {
                if (!response?.data) throw new Error(`预取 ${url} 无有效数据`);
                if (WidgetConfig.DEBUG_LOGGING) console.log(`[预取缓存] 预取成功，获得HTML: ${url}`);
                return response.data;
            })
            .catch(err => {              
                if (WidgetConfig.DEBUG_LOGGING) console.warn(`[预取缓存] 预取网络请求失败 ${url}: ${err.message}`);
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
                console.log(`[HTTP] 第 ${attempts + 1} 次尝试 ${url.substring(0, 80)}...`);
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
                console.warn(`[HTTP] 获取 ${url.substring(0, 80)}... 错误 (尝试 ${attempts}/${maxRetries + 1}):`, error.message);
            }

            if (isAuthError) throw error; 
            if (attempts > maxRetries) throw error; 
            
            const delayMultiplier = attempts; 
            await new Promise(resolve => setTimeout(resolve, retryDelay * delayMultiplier));
        }
    }
    throw new Error(`[HTTP] Max retries reached for ${url}`); 
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
    if (WidgetConfig.DEBUG_LOGGING) console.log(`[TTL Calc] Identifier:`, identifier);

    if (sort === 'trends') return WidgetConfig.TTL_TRENDS_MS;
    
    if (year && year !== "" && month && month !== "" && month !== 'all') { 
        if (isEarlySeason(year, month, currentDate)) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`[TTL Calc] Early season TTL for ${year}-${month}`);
            return WidgetConfig.TTL_SEASON_EARLY_MS;
        } else {
            const seasonStartDate = new Date(parseInt(year, 10), parseInt(month, 10) - 1, 1);
            const monthsSinceSeasonStart = (currentDate.getFullYear() - seasonStartDate.getFullYear()) * 12 + (currentDate.getMonth() - seasonStartDate.getMonth());
            if (monthsSinceSeasonStart > 6) { 
                 if (WidgetConfig.DEBUG_LOGGING) console.log(`[TTL Calc] Archive TTL for ${year}-${month}`);
                 return WidgetConfig.TTL_ARCHIVE_MS;
            }
            if (WidgetConfig.DEBUG_LOGGING) console.log(`[TTL Calc] Late season TTL for ${year}-${month}`);
            return WidgetConfig.TTL_SEASON_LATE_MS;
        }
    } else if (year && year !== "") { 
        if (parseInt(year,10) < currentDate.getFullYear() -1) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`[TTL Calc] Archive TTL for year ${year}`);
            return WidgetConfig.TTL_ARCHIVE_MS; 
        }
        if (WidgetConfig.DEBUG_LOGGING) console.log(`[TTL Calc] Rank TTL for year ${year}`);
        return WidgetConfig.TTL_RANK_MS; 
    }
    if (WidgetConfig.DEBUG_LOGGING) console.log(`[TTL Calc] Default Rank TTL`);
    return WidgetConfig.TTL_RANK_MS; 
}

function normalizeTmdbQuery(query) { if (!query || typeof query !== 'string') return ""; return query.toLowerCase().trim().replace(/[\[\]【】（）()「」『』:：\-－_,\.・]/g, ' ').replace(/\s+/g, ' ').trim();}
function getInfoFromBox($, labelText) { let value = '';const listItems = $('#infobox li');for (let i = 0; i < listItems.length; i++) { const liElement = listItems.eq(i); const tipSpan = liElement.find('span.tip').first(); if (tipSpan.text().trim() === labelText) { value = liElement.clone().children('span.tip').remove().end().text().trim(); return value; } } return value; }
function parseDate(dateStr) { if (!dateStr || typeof dateStr !== 'string') return ''; dateStr = dateStr.trim(); let match; match = dateStr.match(/^(\d{4})年(\d{1,2})月(\d{1,2})日/); if (match) return `${match[1]}-${String(match[2]).padStart(2, '0')}-${String(match[3]).padStart(2, '0')}`; match = dateStr.match(/^(\d{4})年(\d{1,2})月(?!日)/); if (match) return `${match[1]}-${String(match[2]).padStart(2, '0')}-01`; match = dateStr.match(/^(\d{4})年(冬|春|夏|秋)/); if (match) { let m = '01'; if (match[2] === '春') m = '04'; else if (match[2] === '夏') m = '07'; else if (match[2] === '秋') m = '10'; return `${match[1]}-${m}-01`; } match = dateStr.match(/^(\d{4})年(?![\d月春夏秋冬])/); if (match) return `${match[1]}-01-01`; match = dateStr.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})/); if (match) return `${match[1]}-${String(match[2]).padStart(2, '0')}-${String(match[3]).padStart(2, '0')}`; match = dateStr.match(/^(\d{4})[-/](\d{1,2})(?!.*[-/])/); if (match) return `${match[1]}-${String(match[2]).padStart(2, '0')}-01`; match = dateStr.match(/^(\d{4})$/); if (match) return `${match[1]}-01-01`; if (WidgetConfig.DEBUG_LOGGING && dateStr) console.warn(`[日期解析] 无法解析日期字符串: "${dateStr}"`); return '';}

// --- TMDB评分函数 ---
function scoreTmdbResult(result, query, validYear, searchMediaType, originalTitle, chineseTitle) { let currentScore = 0; const resultTitleLower = normalizeTmdbQuery(result.title || result.name); const resultOriginalTitleLower = normalizeTmdbQuery(result.original_title || result.original_name); const queryLower = normalizeTmdbQuery(query); const primaryBgmTitleLower = normalizeTmdbQuery(originalTitle || chineseTitle); if (resultTitleLower === queryLower || resultOriginalTitleLower === queryLower) { currentScore += 15; if (primaryBgmTitleLower && (resultTitleLower === primaryBgmTitleLower || resultOriginalTitleLower === primaryBgmTitleLower)) currentScore += 5; } else if (resultTitleLower.includes(queryLower) || resultOriginalTitleLower.includes(queryLower)) { currentScore += 7; if (primaryBgmTitleLower && (resultTitleLower.includes(primaryBgmTitleLower) || resultOriginalTitleLower.includes(primaryBgmTitleLower))) currentScore += 3; } else { const queryWords = queryLower.split(/\s+/).filter(w => w.length > 1); if (queryWords.length > 0) { const titleWords = new Set([...resultTitleLower.split(/\s+/), ...resultOriginalTitleLower.split(/\s+/)]); let commonWords = 0; queryWords.forEach(qw => { if (titleWords.has(qw)) commonWords++; }); currentScore += (commonWords / queryWords.length) * 6; } else { currentScore -= 2; } } if (validYear) { const resDate = result.release_date || result.first_air_date; if (resDate && resDate.startsWith(String(validYear).substring(0,4))) { const resYear = parseInt(resDate.substring(0, 4), 10); const yearDiff = Math.abs(resYear - validYear); if (yearDiff === 0) currentScore += 6; else if (yearDiff === 1) currentScore += 3; else if (yearDiff <= 2) currentScore += 1; else currentScore -= (yearDiff * 1.5); } else { currentScore -= 2; } } else { currentScore += 1; } if (result.original_language === 'ja' && (searchMediaType === 'tv' || searchMediaType === 'movie')) currentScore += 2.5; currentScore += Math.log10((result.popularity || 0) + 1) * 2.2 + Math.log10((result.vote_count || 0) + 1) * 1.2; if (result.adult) currentScore -= 10; return currentScore; }

// --- TMDB搜索核心逻辑 ---
async function searchTmdb(originalTitle, chineseTitle, listTitle, searchMediaType = 'tv', year = '') {
    const cacheKeyParams = { oT: originalTitle, cT: chineseTitle, lT: listTitle, media: searchMediaType, y: year, v: "original_1.6_perf_final_strict" };
    return CacheUtil.cachedOrFetch('tmdb_search_computed_original_v5_perf_final_strict', cacheKeyParams, async () => {
        let bestOverallMatch = null;
        let highestOverallScore = -Infinity;
        const validYear = year && /^\d{4}$/.test(year) ? parseInt(year, 10) : null;

        if (validYear && (originalTitle || chineseTitle)) {
            const preciseQueryText = normalizeTmdbQuery(originalTitle || chineseTitle);
            if (preciseQueryText) {
                if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB搜索] Stage 1: 查询 "${preciseQueryText}" (年份: ${validYear}, 类型: ${searchMediaType})`);
                try {
                    const params = { query: preciseQueryText, language: "zh-CN", include_adult: false };
                    if (searchMediaType === 'tv') params.first_air_date_year = validYear;
                    else params.primary_release_year = validYear;
                    
                    const tmdbResponse = await fetchWithRetry(`/search/${searchMediaType}`, { params }, 'get', true, WidgetConfig.HTTP_MAIN_RETRIES);
                    const results = tmdbResponse?.results || (Array.isArray(tmdbResponse) ? tmdbResponse : null);
                    
                    if (results?.length > 0) {
                        for (const result of results) {
                            if (searchMediaType === 'tv' && !(result.genre_ids && result.genre_ids.includes(WidgetConfig.TMDB_ANIMATION_GENRE_ID))) {
                                if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB搜索] Stage 1: 跳过非动画结果 (TV搜索) "${result.name || result.title}" (ID: ${result.id})`);
                                continue;
                            }
                            const resDate = result.release_date || result.first_air_date;
                            if (resDate && resDate.startsWith(String(validYear))) {
                                let score = scoreTmdbResult(result, preciseQueryText, validYear, searchMediaType, originalTitle, chineseTitle) + WidgetConfig.TMDB_SEARCH_STAGE1_YEAR_STRICT_SCORE_BOOST;
                                if (score > highestOverallScore) { highestOverallScore = score; bestOverallMatch = result; }
                            }
                        }
                        if (bestOverallMatch && WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB搜索] Stage 1 最佳匹配: ID ${bestOverallMatch.id}, 标题:"${bestOverallMatch.title || bestOverallMatch.name}", 分数: ${highestOverallScore.toFixed(2)}`);
                        if (highestOverallScore >= WidgetConfig.TMDB_SEARCH_STAGE1_HIGH_CONFIDENCE_EXIT_SCORE) {
                            if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB搜索] Stage 1 高分退出 (${highestOverallScore.toFixed(2)} >= ${WidgetConfig.TMDB_SEARCH_STAGE1_HIGH_CONFIDENCE_EXIT_SCORE})`);
                            return bestOverallMatch; 
                        }
                    }
                } catch (e) { if (WidgetConfig.DEBUG_LOGGING) console.error(`[TMDB搜索] Stage 1 API调用错误:`, e.message); }
            }
        }

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
            const refinedBase = refineQueryForSearch(text);
            if (refinedBase) list.add(refinedBase);
            
            if (text && typeof text === 'string') { 
                 const originalNormalized = normalizeTmdbQuery(text); 
                 if (originalNormalized && originalNormalized !== refinedBase) list.add(originalNormalized);

                const firstPartOriginal = normalizeTmdbQuery(text.split(/[:：\-\s（(【\[]/)[0].trim());
                if (firstPartOriginal) list.add(firstPartOriginal);

                const noSeasonOriginal = normalizeTmdbQuery(text.replace(/第.+[期季部篇章]$/g, '').trim());
                if (noSeasonOriginal && noSeasonOriginal !== originalNormalized && noSeasonOriginal !== refinedBase) list.add(noSeasonOriginal);
            }
        };

        addQueryToList(originalTitle, coreQueries);
        addQueryToList(chineseTitle, coreQueries);
        addQueryToList(listTitle, coreQueries);
        
        [originalTitle, chineseTitle, listTitle].forEach(t => { 
            if (t) { 
                coreQueries.add(normalizeTmdbQuery(t.replace(/第.+[期季]$/g, '').trim())); 
                coreQueries.add(normalizeTmdbQuery(t.split(/[:：\-\s（(【\[]/)[0].trim())); 
            } 
        });

        let queriesToProcess = Array.from(coreQueries);
        queriesToProcess = [...new Set(queriesToProcess)].filter(q => q); 
        
        if (queriesToProcess.length > WidgetConfig.MAX_TOTAL_TMDB_QUERIES_TO_PROCESS) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB搜索] 查询词过多 (${queriesToProcess.length}), 截断为 ${WidgetConfig.MAX_TOTAL_TMDB_QUERIES_TO_PROCESS} 个`);
            queriesToProcess = queriesToProcess.slice(0, WidgetConfig.MAX_TOTAL_TMDB_QUERIES_TO_PROCESS);
        }

        if (queriesToProcess.length === 0) {
             if (WidgetConfig.DEBUG_LOGGING && !bestOverallMatch) console.log("[TMDB搜索] Stage 2: 无有效查询词且Stage 1无匹配。");
             return bestOverallMatch; 
        }
        if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB搜索] Stage 2: 查询词 (${queriesToProcess.length}): ${JSON.stringify(queriesToProcess).substring(0,150)}...`);

        const queryPromises = queriesToProcess.map(query => async () => {
            try {
                const params = { query: query, language: "zh-CN", include_adult: false };
                const tmdbSearchResponse = await fetchWithRetry(`/search/${searchMediaType}`, { params }, 'get', true, WidgetConfig.HTTP_MAIN_RETRIES);
                const searchResults = tmdbSearchResponse?.results || (Array.isArray(tmdbSearchResponse) ? tmdbSearchResponse : null);
                let currentBestForQuery = null; let highScoreForQuery = -Infinity;

                if (searchResults?.length > 0) {
                    for (const result of searchResults) {
                        if (searchMediaType === 'tv' && !(result.genre_ids && result.genre_ids.includes(WidgetConfig.TMDB_ANIMATION_GENRE_ID))) {
                             if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB搜索] Stage 2: 跳过非动画结果 (TV搜索) "${result.name || result.title}" (ID: ${result.id}) for query "${query}"`);
                             continue;
                        }
                        const score = scoreTmdbResult(result, query, validYear, searchMediaType, originalTitle, chineseTitle);
                        if (score > highScoreForQuery) { highScoreForQuery = score; currentBestForQuery = result; }
                    }
                }
                return { result: currentBestForQuery, score: highScoreForQuery, query };
            } catch (e) {
                if (WidgetConfig.DEBUG_LOGGING) console.error(`[TMDB搜索] Stage 2 API调用错误，查询 "${query}":`, e.message);
                if (String(e.message).includes("401")||String(e.message).includes("403")) throw e;
                return { result: null, score: -Infinity, query };
            }
        });

        for (let i = 0; i < queryPromises.length; i += WidgetConfig.MAX_CONCURRENT_TMDB_SEARCHES) {
            const batch = queryPromises.slice(i, i + WidgetConfig.MAX_CONCURRENT_TMDB_SEARCHES).map(p => p());
            try {
                const settledResults = await Promise.allSettled(batch);
                for (const sr of settledResults) {
                    if (sr.status === 'fulfilled' && sr.value.result && sr.value.score > highestOverallScore) {
                        highestOverallScore = sr.value.score; bestOverallMatch = sr.value.result;
                        if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB搜索] Stage 2 新总最佳 (来自查询 "${sr.value.query.substring(0,30)}...") ID ${bestOverallMatch.id}, 分数: ${highestOverallScore.toFixed(2)}`);
                    } else if (sr.status === 'rejected') { if (WidgetConfig.DEBUG_LOGGING) console.error(`[TMDB搜索] Stage 2 一个查询Promise被拒绝:`, sr.reason?.message); if (String(sr.reason?.message).includes("401")||String(sr.reason?.message).includes("403")) return null;}
                }
            } catch (batchError) { if (WidgetConfig.DEBUG_LOGGING) console.error(`[TMDB搜索] Stage 2 批处理执行错误:`, batchError.message); if (String(batchError.message).includes("401")||String(batchError.message).includes("403")) return null; }
        }

        if (bestOverallMatch && highestOverallScore >= WidgetConfig.TMDB_SEARCH_MIN_SCORE_THRESHOLD) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB搜索] 最终匹配: ID ${bestOverallMatch.id}, 标题:"${bestOverallMatch.title || bestOverallMatch.name}", 分数: ${highestOverallScore.toFixed(2)}`);
            return bestOverallMatch;
        }
        
        if (WidgetConfig.DEBUG_LOGGING) { const searchTarget = `BGM原名:"${originalTitle||''}" / 中文名:"${chineseTitle||''}"`; console.log(`[TMDB搜索] 未找到满意的TMDB匹配项 (${searchTarget.substring(0,100)}...)。最高得分:${highestOverallScore.toFixed(2)} (阈值:${WidgetConfig.TMDB_SEARCH_MIN_SCORE_THRESHOLD})`);}
        return null; 
    });
}

// --- Bangumi列表项解析 ---
function parseBangumiListItems(htmlContent) { const $ = Widget.html.load(htmlContent); const pendingItems = []; $('ul#browserItemList li.item').each((index, element) => { const $item = $(element); let subjectId = $item.attr('id'); if (subjectId && subjectId.startsWith('item_')) { subjectId = subjectId.substring(5); } else { if(WidgetConfig.DEBUG_LOGGING) console.warn("[BGM列表解析] 无法解析条目ID:", $item.find('h3 a.l').text() || '未知条目'); return; } const titleElement = $item.find('div.inner > h3 > a.l'); const title = titleElement.text().trim(); const detailLink = titleElement.attr('href'); if (!detailLink || !detailLink.trim()) { if(WidgetConfig.DEBUG_LOGGING) console.warn(`[BGM列表解析] 条目 "${title}" (ID: ${subjectId}) 没有有效的详情链接，已跳过。`); return; } const fullDetailLink = `${WidgetConfig.BGM_BASE_URL}${detailLink}`; let listCoverUrl = $item.find('a.subjectCover img.cover').attr('src'); if (listCoverUrl && listCoverUrl.startsWith('//')) { listCoverUrl = 'https:' + listCoverUrl; } else if (!listCoverUrl) { listCoverUrl = ''; } const rating = $item.find('div.inner > p.rateInfo > small.fade').text().trim(); const infoTextFromList = $item.find('div.inner > p.info.tip').text().trim(); pendingItems.push({ id: subjectId, titleFromList: title, detailLink: fullDetailLink, coverFromList: listCoverUrl, ratingFromList: rating || "0", infoTextFromList: infoTextFromList }); }); if (WidgetConfig.DEBUG_LOGGING) console.log(`[BGM列表解析] 从列表页解析到 ${pendingItems.length} 个潜在条目。`); return pendingItems; }

// --- 条目构建与数据集成 ---
function buildBaseItemStructure(pendingItem, detailData) { const {oTitle, cTitle, bPoster, rDate, dMTWidget, fRating} = detailData; const displayTitle = cTitle || oTitle || pendingItem.titleFromList; return { id:String(pendingItem.id), type:"link", title:displayTitle, posterPath:bPoster, backdropPath:'', releaseDate:rDate, mediaType:dMTWidget, rating:fRating, description:"", genreTitle:null, link:pendingItem.detailLink, tmdb_id:null, tmdb_overview:"", tmdb_genres:null, tmdb_tagline:"", tmdb_status:"", tmdb_original_title:"", tmdb_preferred_title:"" }; }

// TMDB完整详情获取队列
let tmdbFullDetailFetchQueue = [];
let isTmdbFullDetailFetchRunning = false;
async function processTmdbFullDetailQueue() {
    if (isTmdbFullDetailFetchRunning || tmdbFullDetailFetchQueue.length === 0) return;
    isTmdbFullDetailFetchRunning = true;
    const batchSize = WidgetConfig.MAX_CONCURRENT_TMDB_FULL_DETAILS_FETCH;
    const currentBatch = tmdbFullDetailFetchQueue.splice(0, batchSize);

    if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB完整详情队列 v3.1.3] 处理批次大小: ${currentBatch.length}`);
    const promises = currentBatch.map(async (task) => {
        const { itemRef, tmdbSearchType, tmdbId } = task;
        try {
            const tmdbDetailResponse = await fetchWithRetry(
                `/${tmdbSearchType}/${tmdbId}`,
                { params: { language: "zh-CN", append_to_response: WidgetConfig.TMDB_APPEND_TO_RESPONSE } },
                'get', true, WidgetConfig.HTTP_MAIN_RETRIES
            );
            const tmdbDetail = tmdbDetailResponse?.data || tmdbDetailResponse; 
            if (tmdbDetail) {
                itemRef.tmdb_overview = tmdbDetail.overview || itemRef.tmdb_overview || ""; 
                const dayPrefixMatch = itemRef.description ? itemRef.description.match(/^\[.*?\]\s*/) : null;
                const dayPrefix = dayPrefixMatch ? dayPrefixMatch[0] : "";
                itemRef.description = `${dayPrefix}${tmdbDetail.overview || itemRef.description.replace(/^\[.*?\]\s*/, '')}`.trim();
                
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
                } else if (!itemRef.tmdb_origin_countries) { 
                    itemRef.tmdb_origin_countries = [];
                }
                if (typeof tmdbDetail.vote_count === 'number') {
                    itemRef.tmdb_vote_count = tmdbDetail.vote_count;
                }

                let bestChineseTitleFromTmdb = '';
                if (tmdbDetail.translations?.translations) { 
                    const chineseTranslation = tmdbDetail.translations.translations.find( t => t.iso_639_1 === 'zh' && t.iso_3166_1 === 'CN' && t.data && (t.data.title || t.data.name) ); 
                    if (chineseTranslation) { bestChineseTitleFromTmdb = (chineseTranslation.data.title || chineseTranslation.data.name).trim(); } 
                }
                itemRef.tmdb_preferred_title = bestChineseTitleFromTmdb || itemRef.title;
                if (bestChineseTitleFromTmdb && bestChineseTitleFromTmdb !== itemRef.title) { 
                    if(WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB完整详情] 更新条目主标题为TMDB中文翻译: "${bestChineseTitleFromTmdb.substring(0,30)}..."`); 
                    itemRef.title = bestChineseTitleFromTmdb; 
                }
                if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB完整详情队列 v3.1.3] 成功获取并更新 TMDB ID ${tmdbId}`);
            }
        } catch (e) { if (WidgetConfig.DEBUG_LOGGING) console.warn(`[TMDB完整详情队列 v3.1.3] 获取 TMDB ID ${tmdbId} 失败:`, e.message); }
    });
    await Promise.allSettled(promises);
    isTmdbFullDetailFetchRunning = false;
    if (tmdbFullDetailFetchQueue.length > 0) Promise.resolve().then(processTmdbFullDetailQueue);
    else if (WidgetConfig.DEBUG_LOGGING) console.log("[TMDB完整详情队列 v3.1.3] 处理完毕。");
}

async function integrateTmdbDataToItem(baseItem, tmdbResult, tmdbSearchType, bgmReleaseDate, bgmRating, bgmDisplayTitle, bgmPosterPathFromBgm) { 
    if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB集成 v3.1.3] BGM "${bgmDisplayTitle.substring(0,30)}..." -> TMDB "${(tmdbResult.title||tmdbResult.name||'').substring(0,30)}..." (ID:${tmdbResult.id})`); 
    
    baseItem.id = String(tmdbResult.id); 
    baseItem.type = "tmdb"; 
    
    baseItem.mediaType = tmdbSearchType; 
    baseItem.tmdb_id = String(tmdbResult.id); 
    baseItem.title = (tmdbResult.title || tmdbResult.name || bgmDisplayTitle).trim(); 
    
    baseItem.posterPath = tmdbResult.poster_path || bgmPosterPathFromBgm; 
    baseItem.backdropPath = tmdbResult.backdrop_path || ''; 
    
    baseItem.releaseDate = parseDate(tmdbResult.release_date || tmdbResult.first_air_date) || bgmReleaseDate; 
    baseItem.rating = tmdbResult.vote_average ? tmdbResult.vote_average.toFixed(1) : bgmRating; 
    baseItem.description = ""; baseItem.genreTitle = null; baseItem.link = null; 
    

    baseItem.tmdb_origin_countries = tmdbResult.origin_country || []; 
    baseItem.tmdb_vote_count = tmdbResult.vote_count; 


    if (WidgetConfig.FETCH_FULL_TMDB_DETAILS) { 
        tmdbFullDetailFetchQueue.push({ itemRef: baseItem, tmdbSearchType, tmdbId: tmdbResult.id, isDailyBroadcastItem: false });
        if (!isTmdbFullDetailFetchRunning) {
            Promise.resolve().then(processTmdbFullDetailQueue);
        }
    } 
}

async function getBangumiDetailCover(subjectId, subjectDetailUrl) { 
    const cacheKey = `bgm_detail_cover_v2.3_${subjectId}`; 
    return CacheUtil.cachedOrFetch(cacheKey, { subjectId }, async () => { 
        if (WidgetConfig.DEBUG_LOGGING) console.log(`[BGM高清封面 v3.1.3] 尝试获取 ${subjectId} 的高清封面从 ${subjectDetailUrl}`); 
        try { 
            const detailHtmlResponse = await fetchWithRetry( subjectDetailUrl, { headers: { "User-Agent": WidgetConfig.BGM_API_USER_AGENT, "Referer": `${WidgetConfig.BGM_BASE_URL}/`, "Accept-Language": "zh-CN,zh;q=0.9" } }, 'get', false, WidgetConfig.HTTP_RETRIES ); 
            if (!detailHtmlResponse?.data) return null; 
            const $ = Widget.html.load(detailHtmlResponse.data); 
            let bPoster = $('#bangumiInfo .infobox a.thickbox.cover[href*="/l/"]').attr('href') || 
                          $('#bangumiInfo .infobox a.thickbox.cover[href*="/g/"]').attr('href') || 
                          $('#bangumiInfo .infobox img.cover[src*="/l/"]').attr('src') || 
                          $('#bangumiInfo .infobox img.cover[src*="/g/"]').attr('src'); 
            if (!bPoster) { bPoster = $('#bangumiInfo .infobox a.thickbox.cover').attr('href') || $('#bangumiInfo .infobox img.cover').attr('src') || ''; } 
            if (bPoster.startsWith('//')) bPoster = 'https:' + bPoster; 
            
            if (bPoster && bPoster.includes('lain.bgm.tv/pic/cover/')) {
                bPoster = bPoster.replace(/\/(m|c|g|s)\//, '/l/'); 
            }

            if (bPoster && !bPoster.startsWith('http') && bPoster.includes('lain.bgm.tv')) { bPoster = (bPoster.startsWith('/') ? WidgetConfig.BGM_BASE_URL : 'https://') + bPoster; } 
            else if (bPoster && !bPoster.startsWith('http')) { if (WidgetConfig.DEBUG_LOGGING) console.warn(`[BGM高清封面 v3.1.3] 非预期的相对路径封面: ${bPoster}`); return null; } 
            return bPoster || null; 
        } catch (e) { if (WidgetConfig.DEBUG_LOGGING) console.warn(`[BGM高清封面 v3.1.3] 获取 ${subjectId} 封面失败: ${e.message}`); return null; } 
    }, { ttl: WidgetConfig.TTL_BGM_DETAIL_COVER_MS }); 
}

async function fetchItemDetails(pendingItem, categoryHint) { 
    if (WidgetConfig.DEBUG_LOGGING) console.log(`[BGM详情 v3.1.3] 开始处理: "${pendingItem.titleFromList.substring(0,50)}..." (BGM ID: ${pendingItem.id})`); 
    let detailHtmlResponse; 
    try { 
        detailHtmlResponse = await fetchWithRetry( pendingItem.detailLink, { headers: { "User-Agent": WidgetConfig.BGM_API_USER_AGENT, "Referer": `${WidgetConfig.BGM_BASE_URL}/`, "Accept-Language": "zh-CN,zh;q=0.9" } }, 'get', false, WidgetConfig.HTTP_MAIN_RETRIES ); 
        if (!detailHtmlResponse?.data) { throw new Error(`Bangumi详情页 ${pendingItem.detailLink} 响应数据为空或无效`); } 
    } catch (htmlError) { console.error(`[BGM详情 v3.1.3] 无法获取Bangumi HTML内容 (ID ${pendingItem.id}, 链接 ${pendingItem.detailLink}):`, htmlError.message); return null; } 
    const detailHtml = detailHtmlResponse.data; 
    try { 
        const $ = Widget.html.load(detailHtml); 
        const oTitle = $('h1.nameSingle > a').first().text().trim(); 
        const cTitle = getInfoFromBox($, "中文名:"); 
        let bPoster = $('#bangumiInfo .infobox a.thickbox.cover').attr('href') || $('#bangumiInfo .infobox img.cover').attr('src') || pendingItem.coverFromList || ''; 
        if (bPoster.startsWith('//')) bPoster = 'https:' + bPoster; 
        if (bPoster && bPoster.includes('lain.bgm.tv/pic/cover/')) { 
            bPoster = bPoster.replace(/\/(m|c|g|s)\//, '/l/');
        }

        let rDateStr = getInfoFromBox($, "放送开始:") || getInfoFromBox($, "上映年度:") || getInfoFromBox($, "发售日期:") || getInfoFromBox($, "发行日期:"); 
        let rDate = parseDate(rDateStr); 
        if (!rDate && pendingItem.infoTextFromList) { const dateMatchInInfo = pendingItem.infoTextFromList.match(/(\d{4}年\d{1,2}月\d{1,2}日|\d{4}年\d{1,2}月|\d{4}年[春夏秋冬]|\d{4}年)/); if (dateMatchInInfo?.[0]) rDate = parseDate(dateMatchInInfo[0]); } 
        const bgmMTDisplay = ($('h1.nameSingle small.grey').first().text().trim()||"").toLowerCase(); 
        let dMTWidget = categoryHint; let tmdbSType = ''; 
        if (categoryHint === 'anime' || categoryHint === 'real') { if (bgmMTDisplay.includes('movie') || bgmMTDisplay.includes('剧场版') || bgmMTDisplay.includes('映画')) { dMTWidget = 'movie'; } else { dMTWidget = 'tv'; } tmdbSType = dMTWidget; } 
        const fRating = ($('#panelInterestWrapper .global_rating .number').text().trim()) || pendingItem.ratingFromList || "0"; 
        const yearForTmdb = rDate ? rDate.substring(0, 4) : ''; 
        const item = buildBaseItemStructure(pendingItem, {oTitle, cTitle, bPoster, rDate, dMTWidget, fRating}); 
        if (tmdbSType) { 
            const tmdbRes = await searchTmdb(oTitle, cTitle, pendingItem.titleFromList, tmdbSType, yearForTmdb); 
            if (tmdbRes?.id) { await integrateTmdbDataToItem(item, tmdbRes, tmdbSType, rDate, fRating, item.title, bPoster); } 
            else { if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB匹配 v3.1.3] 未能匹配BGM条目: "${item.title.substring(0,30)}..." (BGM ID: ${pendingItem.id}). 将使用BGM数据。`); } 
        } else { if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB搜索 v3.1.3] 跳过非影视类型条目: "${item.title.substring(0,30)}..." (分类: ${categoryHint}, BGM ID: ${pendingItem.id})`); } 
        if (WidgetConfig.DEBUG_LOGGING) { const logItemOutput = {...item}; if(logItemOutput.tmdb_overview?.length>30) logItemOutput.tmdb_overview = logItemOutput.tmdb_overview.substring(0,27)+"..."; console.log(`[BGM详情 v3.1.3] 处理完成: BGM_ID:${pendingItem.id}, Final_ID:${logItemOutput.id}, 类型:${logItemOutput.type}, 标题:"${logItemOutput.title.substring(0,30)}"...`); } 
        return item; 
    } catch (processingError) { console.error(`[BGM详情 v3.1.3] 处理已获取的BGM HTML时发生错误 (ID ${pendingItem.id}, 标题 "${pendingItem.titleFromList.substring(0,30)}..."):`, processingError.message, processingError.stack?.substring(0,100)); return buildBaseItemStructure(pendingItem, {oTitle: pendingItem.titleFromList, cTitle:'', bPoster: pendingItem.coverFromList, rDate:'', dMTWidget: categoryHint, fRating: pendingItem.ratingFromList}); } 
}

async function processBangumiPage(url, categoryHint, currentPageString, rankingContextInfo = {}) { 
    const currentPage = currentPageString ? parseInt(currentPageString, 10) : 0; 
    if (WidgetConfig.DEBUG_LOGGING) console.log(`[BGM处理 v3.1.3] 列表页: ${url} (当前页: ${currentPage > 0 ? currentPage : '未知/1'})`); 
    let listHtml; 
    const commonHeaders = { "User-Agent": WidgetConfig.BGM_API_USER_AGENT, "Referer": `${WidgetConfig.BGM_BASE_URL}/`, "Accept-Language": "zh-CN,zh;q=0.9" }; 
    const prefetchedHtmlPromise = PrefetchCache.get(url); 
    if (prefetchedHtmlPromise) { if (WidgetConfig.DEBUG_LOGGING) console.log(`[BGM处理 v3.1.3] 使用预取缓存中的HTML Promise: ${url}`); try { listHtml = await prefetchedHtmlPromise; } catch (e) { if (WidgetConfig.DEBUG_LOGGING) console.warn(`[BGM处理 v3.1.3] 预取HTML的Promise解析失败 (${url}): ${e.message}。将尝试重新获取。`); listHtml = null; } } 
    if (!listHtml) { if (WidgetConfig.DEBUG_LOGGING && !prefetchedHtmlPromise) console.log(`[BGM处理 v3.1.3] 未在预取缓存中找到或预取失败，正常获取HTML: ${url}`); try { const listHtmlResp = await fetchWithRetry(url, { headers: commonHeaders }, 'get', false, WidgetConfig.HTTP_MAIN_RETRIES); if (!listHtmlResp?.data) throw new Error("列表页响应数据为空或无效"); listHtml = listHtmlResp.data; } catch (e) { console.error(`[BGM处理 v3.1.3] 获取列表页 ${url} 失败:`, e.message); throw new Error(`请求Bangumi列表页失败: ${e.message}`); } } 
    if (currentPage > 0) { const nextPageNum = currentPage + 1; let nextPageUrl; if (url.includes("page=")) { nextPageUrl = url.replace(/page=\d+/, `page=${nextPageNum}`); } else if (url.includes("?")) { nextPageUrl = `${url}&page=${nextPageNum}`; } else { nextPageUrl = `${url}?page=${nextPageNum}`; } if (nextPageUrl && nextPageUrl !== url) { if (WidgetConfig.DEBUG_LOGGING) console.log(`[BGM处理 v3.1.3] 触发下一页 (${nextPageNum}) 的HTML预取: ${nextPageUrl}`); PrefetchCache.fetchAndCacheHtml(nextPageUrl, commonHeaders).catch(()=>{}); } } 
    const pendingItems = parseBangumiListItems(listHtml); 
    if (pendingItems.length === 0) { if (WidgetConfig.DEBUG_LOGGING) console.log("[BGM处理 v3.1.3] 从HTML未解析到任何条目。"); return []; } 
    if (WidgetConfig.DEBUG_LOGGING) console.log(`[BGM处理 v3.1.3] 解析到 ${pendingItems.length} 个条目。开始并发获取详情 (最大并发: ${WidgetConfig.MAX_CONCURRENT_DETAILS_FETCH})...`); 
    const results = []; 
    for (let i = 0; i < pendingItems.length; i += WidgetConfig.MAX_CONCURRENT_DETAILS_FETCH) { 
        const batch = pendingItems.slice(i, i + WidgetConfig.MAX_CONCURRENT_DETAILS_FETCH); 
        if (WidgetConfig.DEBUG_LOGGING) console.log(`[BGM处理 v3.1.3] 处理详情批次 ${Math.floor(i/WidgetConfig.MAX_CONCURRENT_DETAILS_FETCH)+1} (数量: ${batch.length})`); 
        const detailPromises = batch.map(item => CacheUtil.cachedOrFetch( 'item_detail_computed_v3.1.3_final', { itemId: item.id, category: categoryHint, scriptVer: WidgetMetadata.version }, () => fetchItemDetails(item, categoryHint), { calculateTTL: calculateContentTTL, context: { currentDate: new Date() }, ttlIdentifier: rankingContextInfo } ).catch(e => { console.error(`[BGM处理 v3.1.3] fetchItemDetails 或其缓存包装执行失败 (BGM ID: ${item.id}): `, e.message); return null; }) ); 
        const settledResults = await Promise.allSettled(detailPromises); 
        settledResults.forEach(settledResult => { if (settledResult.status === 'fulfilled' && settledResult.value) { results.push(settledResult.value); } else if (settledResult.status === 'rejected') { console.error(`[BGM处理 v3.1.3] 一个条目详情Promise被拒绝:`, settledResult.reason?.message); } }); 
    } 
    if (WidgetConfig.DEBUG_LOGGING) console.log(`[BGM处理 v3.1.3] 列表页处理完成。返回 ${results.length} 条有效结果.`); 
    return results; 
}

async function fetchRecentHot(params = {}) { const category = params.category || "anime"; const page = params.page || "1"; const url = `${WidgetConfig.BGM_BASE_URL}/${category}/browser/?sort=trends&page=${page}`; if (WidgetConfig.DEBUG_LOGGING) console.log(`[模式 v3.1.3] 获取近期热门: 分类=${category}, 页=${page}`); try { return await processBangumiPage(url, category, page, { category, sort: 'trends' }); } catch (error) { console.error(`[模式 v3.1.3] fetchRecentHot(分类:${category}, 页码:${page}) 发生顶层错误:`, error.message); return []; } }

// --- 放送/发售时段排行 ---
// 确保 rankingContextInfo 变量正确定义和传递
async function fetchAirtimeRanking(params = {}) {
    const category = params.category || "anime";
    const year = params.year || ""; 
    const month = params.month || "all"; 
    const sort = params.sort || "rank";
    const page = params.page || "1";
    let url;

    if (WidgetConfig.DEBUG_LOGGING) {
        console.log(`[fetchAirtimeRanking PARAMS v3.1.3] Category: ${category}, Year: '${year}', Month: '${month}', Sort: ${sort}, Page: ${page}`);
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
            console.warn(`[模式 v3.1.3] 时段排行提供的年份 "${year}" 格式无效。将浏览所有年份。`);
        }
    }

    if (WidgetConfig.DEBUG_LOGGING) {
        console.log(`[模式 v3.1.3] 获取时段排行: URL=${url}`);
    }

    try {
        const rankingContextInfo = { category, year, month, sort }; 
        if (WidgetConfig.DEBUG_LOGGING) console.log('[fetchAirtimeRanking v3.1.3] rankingContextInfo:', rankingContextInfo);
        return await processBangumiPage(url, category, page, rankingContextInfo); 
    } catch (error) {
        console.error(`[模式 v3.1.3] fetchAirtimeRanking(分类:${category}, 年:${year}, 月:${month}, 排序:${sort}, 页:${page}) 发生顶层错误:`, error.message, error.stack);
        return [];
    }
}

// --- TMDB完整详情获取队列处理 ---
async function processTmdbFullDetailQueue() {
    if (isTmdbFullDetailFetchRunning || tmdbFullDetailFetchQueue.length === 0) return;
    isTmdbFullDetailFetchRunning = true;
    const batchSize = WidgetConfig.MAX_CONCURRENT_TMDB_FULL_DETAILS_FETCH;
    const currentBatch = tmdbFullDetailFetchQueue.splice(0, batchSize);

    if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB完整详情队列 v3.1.3] 处理批次大小: ${currentBatch.length}`);
    const promises = currentBatch.map(async (task) => {
        const { itemRef, tmdbSearchType, tmdbId } = task; 
        try {
            const tmdbDetailResponse = await fetchWithRetry(
                `/${tmdbSearchType}/${tmdbId}`,
                { params: { language: "zh-CN", append_to_response: WidgetConfig.TMDB_APPEND_TO_RESPONSE } },
                'get', true, WidgetConfig.HTTP_MAIN_RETRIES
            );
            const tmdbDetail = tmdbDetailResponse?.data || tmdbDetailResponse; 
            if (tmdbDetail) {
                itemRef.tmdb_overview = tmdbDetail.overview || itemRef.tmdb_overview || ""; 
                const dayPrefixMatch = itemRef.description ? itemRef.description.match(/^\[.*?\]\s*/) : null;
                const dayPrefix = dayPrefixMatch ? dayPrefixMatch[0] : "";
                itemRef.description = `${dayPrefix}${tmdbDetail.overview || itemRef.description.replace(/^\[.*?\]\s*/, '')}`.trim();
                
                if (tmdbDetail.genres?.length > 0) {
                    itemRef.tmdb_genres = tmdbDetail.genres.map(g => g.name).join(', ');
                    itemRef.genreTitle = itemRef.tmdb_genres; 
                }
                itemRef.tmdb_tagline = tmdbDetail.tagline || "";
                itemRef.tmdb_status = tmdbDetail.status || "";
                itemRef.tmdb_original_title = tmdbDetail.original_title || tmdbDetail.original_name || "";
                
                // 提取国家信息
                if (tmdbDetail.origin_country && Array.isArray(tmdbDetail.origin_country) && tmdbDetail.origin_country.length > 0) {
                    itemRef.tmdb_origin_countries = tmdbDetail.origin_country;
                } else if (tmdbDetail.production_countries && Array.isArray(tmdbDetail.production_countries) && tmdbDetail.production_countries.length > 0) {
                    itemRef.tmdb_origin_countries = tmdbDetail.production_countries.map(pc => pc.iso_3166_1);
                } else if (!itemRef.tmdb_origin_countries) { 
                    itemRef.tmdb_origin_countries = [];
                }
                // 提取投票数
                if (typeof tmdbDetail.vote_count === 'number') {
                    itemRef.tmdb_vote_count = tmdbDetail.vote_count;
                }

                let bestChineseTitleFromTmdb = '';
                if (tmdbDetail.translations?.translations) { 
                    const chineseTranslation = tmdbDetail.translations.translations.find( t => t.iso_639_1 === 'zh' && t.iso_3166_1 === 'CN' && t.data && (t.data.title || t.data.name) ); 
                    if (chineseTranslation) { bestChineseTitleFromTmdb = (chineseTranslation.data.title || chineseTranslation.data.name).trim(); } 
                }
                itemRef.tmdb_preferred_title = bestChineseTitleFromTmdb || itemRef.title;
                if (bestChineseTitleFromTmdb && bestChineseTitleFromTmdb !== itemRef.title) { 
                    if(WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB完整详情] 更新条目主标题为TMDB中文翻译: "${bestChineseTitleFromTmdb.substring(0,30)}..."`); 
                    itemRef.title = bestChineseTitleFromTmdb; 
                }
                if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB完整详情队列 v3.1.3] 成功获取并更新 TMDB ID ${tmdbId}`);
            }
        } catch (e) { if (WidgetConfig.DEBUG_LOGGING) console.warn(`[TMDB完整详情队列 v3.1.3] 获取 TMDB ID ${tmdbId} 失败:`, e.message); }
    });
    await Promise.allSettled(promises);
    isTmdbFullDetailFetchRunning = false;
    if (tmdbFullDetailFetchQueue.length > 0) Promise.resolve().then(processTmdbFullDetailQueue);
    else if (WidgetConfig.DEBUG_LOGGING) console.log("[TMDB完整详情队列 v3.1.3] 处理完毕。");
}

// --- 每日放送后台增强函数 ---
async function enhanceCalendarItemInBackground(apiItemData, initialVideoItem) {
    if (WidgetConfig.DEBUG_LOGGING) console.log(`[每日放送增强 v3.1.4] 处理 BGM ID: ${apiItemData.id}, 初始标题: ${initialVideoItem.title}`);
    
    let tmdbResultForLogic = null;
    let itemChangedByTmdb = false;
    let finalPosterPath = initialVideoItem.posterPath; 
    let tmdbSearchType = ''; 

    try {
        const { id: bgmId, name: bgmName, name_cn: bgmNameCn, air_date: bgmAirDate, type: bgmApiType, url: bgmUrl } = apiItemData;
        const itemTitleForSearch = bgmNameCn || bgmName;
        const itemYear = bgmAirDate ? bgmAirDate.substring(0, 4) : '';
        
        const typeMappingFromApi = { 1: "book", 2: "anime", 3: "music", 4: "game", 6: "real" };
        const bgmCategoryHint = typeMappingFromApi[bgmApiType] || 'unknown';

        if (bgmCategoryHint === 'anime') tmdbSearchType = 'tv';
        else if (bgmCategoryHint === 'real') tmdbSearchType = 'movie'; 

        if (tmdbSearchType) {
            tmdbResultForLogic = await searchTmdb(bgmName, bgmNameCn, itemTitleForSearch, tmdbSearchType, itemYear);
        }

        if (tmdbResultForLogic && tmdbResultForLogic.id) {
            itemChangedByTmdb = true;
            if (WidgetConfig.DEBUG_LOGGING) console.log(`[每日放送增强 v3.1.4] TMDB 匹配成功 for BGM ID ${bgmId}: TMDB ID ${tmdbResultForLogic.id}, 类型: ${tmdbSearchType}`);
            
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
                // 请求时不附加过多内容，加快速度
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
                     if (WidgetConfig.DEBUG_LOGGING) console.log(`[每日放送增强 v3.1.4] TMDB ID ${tmdbResultForLogic.id} 初步国家/投票数信息:`, initialVideoItem.tmdb_origin_countries, initialVideoItem.tmdb_vote_count);
                }
            } catch (e) {
                if (WidgetConfig.DEBUG_LOGGING) console.warn(`[每日放送增强 v3.1.4] 获取TMDB ID ${tmdbResultForLogic.id} 初步国家/投票数信息失败: ${e.message}`);
            }

            if (WidgetConfig.FETCH_FULL_TMDB_DETAILS) { 
                tmdbFullDetailFetchQueue.push({itemRef: initialVideoItem, tmdbSearchType, tmdbId: tmdbResultForLogic.id, isDailyBroadcastItem: true });
                if(!isTmdbFullDetailFetchRunning) Promise.resolve().then(processTmdbFullDetailQueue);
            }
        } else {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`[每日放送增强 v3.1.4] TMDB 未匹配 BGM ID ${bgmId}.`);
            initialVideoItem.bgm_score = apiItemData.rating?.score || 0;
            initialVideoItem.bgm_rating_total = apiItemData.rating?.total || 0;
        }

        if (!itemChangedByTmdb || (itemChangedByTmdb && tmdbResultForLogic && !tmdbResultForLogic.poster_path)) {
            const bgmDetailUrl = bgmUrl || `${WidgetConfig.BGM_BASE_URL}/subject/${bgmId}`;
            const bgmHighResCover = await getBangumiDetailCover(bgmId, bgmDetailUrl);
            if (bgmHighResCover) {
                if (WidgetConfig.DEBUG_LOGGING) console.log(`[每日放送增强 v3.1.4] BGM 高清封面 for ${bgmId}: ${bgmHighResCover}`);
                finalPosterPath = bgmHighResCover;
            }
        }
        initialVideoItem.posterPath = finalPosterPath; 

        CacheUtil.set('calendar_item_final_display', String(bgmId), Promise.resolve({...initialVideoItem}), WidgetConfig.TTL_CALENDAR_ITEM_ENHANCED_MS);
        
        if (WidgetConfig.DEBUG_LOGGING) console.log(`[每日放送增强 v3.1.4] 完成处理 BGM ID: ${bgmId}. 最终ID: ${initialVideoItem.id}, 类型: ${initialVideoItem.type}`);
        return initialVideoItem; 

    } catch (error) {
        console.error(`[每日放送增强 v3.1.4] 错误处理 BGM ID ${apiItemData.id}:`, error.message, error.stack);
        initialVideoItem.bgm_score = apiItemData.rating?.score || 0;
        initialVideoItem.bgm_rating_total = apiItemData.rating?.total || 0;
        return initialVideoItem; 
    }
}



// --- 每日放送主函数  ---
async function fetchDailyCalendarApi(params = {}) {
    const filterType = params.filterType || "today";
    const specificWeekdayParam = (filterType === "specific_day") ? params.specificWeekday : null;
    const sortOrder = params.dailySortOrder || "popularity_rat_bgm";
    const regionFilter = params.dailyRegionFilter || "all";

    if (WidgetConfig.DEBUG_LOGGING) console.log(`[模式 v3.1.6] 获取每日放送, 筛选: ${filterType}, 指定星期: ${specificWeekdayParam}, 排序: ${sortOrder}, 区域: ${regionFilter}`);
    
    const actualApiUrl = `https://api.bgm.tv/calendar`;
    try {
        const apiResponse = await CacheUtil.cachedOrFetch(
            'bgm_calendar_api', 
            'weekly_broadcast_data_v1.2', 
            async () => {
                const response = await Widget.http.get(actualApiUrl, { headers: { "User-Agent": WidgetConfig.BGM_API_USER_AGENT } });
                if (!response || !response.data) throw new Error("Bangumi Calendar API响应为空或无效");
                if (WidgetConfig.DEBUG_LOGGING) console.log('[每日放送 v3.1.6] Calendar API 原始响应:', JSON.stringify(response.data).substring(0, 500));
                return response.data;
            }, 
            { ttl: WidgetConfig.TTL_CALENDAR_API_MS }
        );

        if (!Array.isArray(apiResponse)) {
            console.error("[每日放送 API v3.1.6] 响应数据格式不正确"); return [];
        }

        let filteredApiItems = [];
        const today = new Date();
        const currentJsDay = today.getDay(); 
        const jsDayToBgmApiId = { 0: 7, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6 }; 
        const bgmApiTodayId = jsDayToBgmApiId[currentJsDay];
        let targetBgmApiWeekdayId = null;

        if (filterType === "today") {
            targetBgmApiWeekdayId = bgmApiTodayId;
        } else if (filterType === "specific_day") {
            targetBgmApiWeekdayId = specificWeekdayParam ? parseInt(specificWeekdayParam, 10) : -1; 
            if (isNaN(targetBgmApiWeekdayId) || targetBgmApiWeekdayId < 1 || targetBgmApiWeekdayId > 7) {
                if (WidgetConfig.DEBUG_LOGGING) console.warn(`[每日放送 v3.1.6] 无效的指定星期: ${specificWeekdayParam}，返回空列表。`);
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
                    if (targetBgmApiWeekdayId !== -1 && dayOfWeekId === targetBgmApiWeekdayId) includeDay = true; 
                    break;
                case "mon_thu": if (dayOfWeekId >= 1 && dayOfWeekId <= 4) includeDay = true; break;
                case "fri_sun": if (dayOfWeekId >= 5 && dayOfWeekId <= 7) includeDay = true; break;
                case "all_week": default: includeDay = true; break;
            }
            if (includeDay) {
                filteredApiItems.push(...dayData.items.map(item => ({ ...item, weekday_cn: dayData.weekday?.cn || `周${dayOfWeekId}` })));
            }
        });
        
        // 可选的严格年份过滤：如果API返回了非当年的数据，可以在这里过滤
        // 注意：这可能过滤掉跨年番或API本身数据问题，谨慎使用
        // const currentYear = new Date().getFullYear();
        // filteredApiItems = filteredApiItems.filter(apiItem => {
        //     if (apiItem.air_date) {
        //         const itemYear = parseInt(apiItem.air_date.substring(0, 4), 10);
        //         return itemYear === currentYear; // 只保留当年的
        //     }
        //     return true; // 没有播出日期的暂时保留
        // });
        // if (WidgetConfig.DEBUG_LOGGING) console.log(`[每日放送 v3.1.6] 年份过滤后 (${currentYear}) 剩余条目: ${filteredApiItems.length}`);


        if (filteredApiItems.length === 0 && WidgetConfig.DEBUG_LOGGING) {
            console.log(`[每日放送 v3.1.6] 筛选后无任何条目 (filterType: ${filterType}, targetBgmApiWeekdayId: ${targetBgmApiWeekdayId})`);
        }
        if (filteredApiItems.length === 0) return [];


        const resultsToReturn = [];
        const enhancementPromises = [];
        const typeMapping = { 1: "book", 2: "anime", 3: "music", 4: "game", 6: "real" };

        for (const item of filteredApiItems) {
            const bgmIdStr = String(item.id);
            const cachedFinalItem = await CacheUtil.get('calendar_item_final_display', bgmIdStr);
            if (cachedFinalItem) {
                cachedFinalItem.bgm_rating_total = cachedFinalItem.bgm_rating_total !== undefined ? cachedFinalItem.bgm_rating_total : (item.rating?.total || 0);
                cachedFinalItem.bgm_score = cachedFinalItem.bgm_score !== undefined ? cachedFinalItem.bgm_score : (item.rating?.score || 0);
                cachedFinalItem.bgm_air_date = cachedFinalItem.bgm_air_date || item.air_date;
                if (WidgetConfig.DEBUG_LOGGING) console.log(`[每日放送 v3.1.6] 使用缓存的增强条目 BGM ID: ${bgmIdStr}`);
                resultsToReturn.push(cachedFinalItem);
                continue; 
            }

            let cover = item.images?.large || item.images?.common || item.images?.medium || item.images?.grid || item.images?.small || "";
            if (cover.startsWith("//")) cover = "https:" + cover;
            if (cover && cover.includes('lain.bgm.tv/pic/cover/')) {
                cover = cover.replace(/\/(m|c|g|s)\//, '/l/');
            }
            const mediaTypeNum = item.type;
            const mediaTypeStr = typeMapping[mediaTypeNum] || "unknown";
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
                bgm_collection_count: item.collection?.collect || 0,
                bgm_rating_total: item.rating?.total || 0, // 用于BGM评分人数排序
                bgm_score: item.rating?.score || 0,       // 用于BGM评分排序
                bgm_air_date: item.air_date,              // 用于BGM放送日期排序
                tmdb_origin_countries: [],
                tmdb_vote_count: null 
            };
            
            enhancementPromises.push(enhanceCalendarItemInBackground(item, videoItem));
            resultsToReturn.push(videoItem); 
        }
        
        if (enhancementPromises.length > 0) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`[每日放送 v3.1.6] 等待 ${enhancementPromises.length} 个条目的核心增强...`);
            await Promise.allSettled(enhancementPromises);
            if (WidgetConfig.DEBUG_LOGGING) console.log(`[每日放送 v3.1.6] 核心增强完成.`);
        }
        
        // --- 排序逻辑  ---
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
                } catch (e) { console.error("[每日放送排序错误 v3.1.6]", e); return 0; }
                return 0;
            });
        }

        // --- 区域筛选逻辑 ---
        let finalFilteredResults = resultsToReturn;
        if (regionFilter !== "all") {
            finalFilteredResults = resultsToReturn.filter(item => {
                if (item.type === "tmdb" && item.tmdb_id) {
                    const countries = item.tmdb_origin_countries || []; 
                    if (WidgetConfig.DEBUG_LOGGING && countries.length > 0) console.log(`[区域筛选 v3.1.6] TMDB条目 ${item.title} 国家: ${countries.join(',')}`);
                    if (countries.length === 0) return regionFilter === "OTHER"; 
                    if (regionFilter === "JP") return countries.includes("JP");
                    if (regionFilter === "CN") return countries.includes("CN");
                    if (regionFilter === "US_EU") return countries.some(c => ["US", "GB", "FR", "DE", "CA", "AU", "ES", "IT"].includes(c)); 
                    if (regionFilter === "OTHER") {
                        const isJPCNUSEU = countries.includes("JP") || countries.includes("CN") || countries.some(c => ["US", "GB", "FR", "DE", "CA", "AU", "ES", "IT"].includes(c));
                        return !isJPCNUSEU;
                    }
                    return false; 
                } else { 
                    return regionFilter === "all" || regionFilter === "OTHER"; 
                }
            });
        }
        
        if (WidgetConfig.DEBUG_LOGGING) console.log(`[每日放送 API v3.1.6] 最终处理完成，返回 ${finalFilteredResults.length} 个条目。`);
        return finalFilteredResults;

    } catch (error) {
        console.error(`[模式 v3.1.6] fetchDailyCalendarApi 发生错误:`, error.message, error.stack?.substring(0,200));
        return [];
    }
}
