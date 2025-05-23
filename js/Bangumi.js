// --- 小部件配置 ---
const WidgetConfig = {
    MAX_CONCURRENT_DETAILS_FETCH: 6,    // 同时获取Bangumi条目详情的最大并发数
    MAX_CONCURRENT_TMDB_SEARCHES: 4,    // TMDB搜索 Stage 2 的最大并发数
    HTTP_RETRIES: 1,                    // 预取HTML时的HTTP重试次数
    HTTP_MAIN_RETRIES: 2,               // 主流程API请求的HTTP重试次数
    HTTP_RETRY_DELAY: 1000,             // 重试间的延迟时间-毫秒
    FETCH_FULL_TMDB_DETAILS: true,      // 是否从TMDB获取详细信息
    TMDB_APPEND_TO_RESPONSE: "translations", 
    TMDB_SEARCH_STAGE1_YEAR_STRICT_SCORE_BOOST: 12, // TMDB搜索Stage 1年份严格匹配的得分提升
    TMDB_SEARCH_STAGE1_HIGH_CONFIDENCE_EXIT_SCORE: 23, // TMDB搜索Stage 1高分提前退出的阈值
    TMDB_SEARCH_MIN_SCORE_THRESHOLD: 6, // TMDB匹配有效的最低分数
    CACHE_TTL_MS: 25 * 60 * 1000,       // 计算结果缓存TTL时间
    PREFETCH_CACHE_TTL_MS: 2 * 60 * 1000, // 预取HTML缓存TTL时间
    MAX_PREFETCHED_PAGES: 5,            // 预取缓存中保留的页面HTML数量
    DEBUG_LOGGING: false                //日志
};

var WidgetMetadata = {
    id: "bangumi_charts_tmdb_v2", 
    title: "Bangumi 热门榜单",
    description: "从Bangumi获取榜单，智能匹配TMDB数据",
    author: "Autism ",
    version: "1.0.1",
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
        }
    ]
};

// --- 缓存工具 (负责缓存复杂计算结果) ---
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
    set: function(type, identifier, valuePromise) {
        const key = this._generateKey(type, identifier);
        this.pendingPromises.set(key, valuePromise);
        return valuePromise.then(value => {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`[CacheUtil] 设置缓存: ${key.substring(0, 80)}...`);
            this.cache.set(key, { value: value, expiry: Date.now() + WidgetConfig.CACHE_TTL_MS });
            this.pendingPromises.delete(key);
            return value;
        }).catch(error => {
            if (WidgetConfig.DEBUG_LOGGING) console.warn(`[CacheUtil] Promise执行失败，从pending移除: ${key.substring(0, 80)}...`, error.message);
            this.pendingPromises.delete(key);
            throw error;
        });
    },
    cachedOrFetch: function(cacheType, identifier, fetchFn) {
        const cachedPromise = this.get(cacheType, identifier);
        if (cachedPromise) return cachedPromise;
        return this.set(cacheType, identifier, fetchFn());
    }
};

// --- 智能预取缓存 (负责缓存下一页原始HTML) ---
const PrefetchCache = {
    prefetchedHtml: new Map(), // key: url, value: { promise: Promise<string>, timestamp: number, inProgress: boolean }
    
    get: function(url) {
        const entry = this.prefetchedHtml.get(url);
        if (entry && (Date.now() - entry.timestamp < WidgetConfig.PREFETCH_CACHE_TTL_MS)) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`[预取缓存] 命中: ${url}`);
            return entry.promise; // 返回存储的Promise
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

        // 维持缓存大小，移除最旧的非进行中条目
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
        // 如果存在且仍在进行中，或未过期，则返回其promise
        if (existingEntry && (existingEntry.inProgress || (Date.now() - existingEntry.timestamp < WidgetConfig.PREFETCH_CACHE_TTL_MS))) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`[预取缓存] 使用现有预取Promise: ${url}`);
            return existingEntry.promise;
        }
        if (existingEntry) { 
             this.prefetchedHtml.delete(url);
        }

        // 发起新的预取请求 (使用较低的重试次数)
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


// --- HTTP请求封装 (可指定重试次数) ---
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
            return await api[method](url, options);
        } catch (error) {
            attempts++;
            const isAuthError = String(error.message).includes("401") || String(error.message).includes("403");

            if (WidgetConfig.DEBUG_LOGGING || attempts > maxRetries || isAuthError) {
                console.warn(`[HTTP] 获取 ${url.substring(0, 80)}... 错误 (尝试 ${attempts}/${maxRetries + 1}):`, error.message);
            }

            if (isAuthError) throw error;
            if (attempts > maxRetries) throw error;
            
            // TMDB请求使用指数退避，其他（如BGM HTML）使用固定延迟首次重试，之后也指数
            // const delayMultiplier = (isTmdb || attempts > 1) ? attempts : 1; // 调整：BGM首次重试也用固定，之后指数
            const delayMultiplier = attempts; 
            await new Promise(resolve => setTimeout(resolve, retryDelay * delayMultiplier));
        }
    }
}


// --- TMDB搜索核心逻辑 (其计算结果由CacheUtil缓存) ---
async function searchTmdb(originalTitle, chineseTitle, listTitle, searchMediaType = 'tv', year = '') {
    const cacheKeyParams = { oT: originalTitle, cT: chineseTitle, lT: listTitle, media: searchMediaType, y: year, v: "1.5" }; // 加版本号避免旧缓存
    return CacheUtil.cachedOrFetch('tmdb_search_computed_v4', cacheKeyParams, async () => {
        const queries = new Set();
        function addQuery(queryText) { const norm = normalizeTmdbQuery(queryText); if (norm) queries.add(norm); }
        addQuery(originalTitle); addQuery(chineseTitle);
        [originalTitle, chineseTitle, listTitle].forEach(t => { if (t) { addQuery(t.replace(/第.+[期季]$/g, '').trim()); addQuery(t.split(/[:：\-\s（(【\[]/)[0].trim()); } });
        addQuery(listTitle);

        if (queries.size === 0) { if (WidgetConfig.DEBUG_LOGGING) console.log("[TMDB搜索] 无有效查询词。"); return null; }
        const uniqueQueries = Array.from(queries);
        if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB搜索] 类型:${searchMediaType}, 年份:${year}. 查询集(${uniqueQueries.length}): ${JSON.stringify(uniqueQueries).substring(0,100)}...`);

        let bestOverallMatch = null;
        let highestOverallScore = -Infinity;
        const validYear = year && /^\d{4}$/.test(year) ? parseInt(year, 10) : null;

        // Stage 1: 精确年份搜索
        if (validYear && (originalTitle || chineseTitle)) {
            const preciseQuery = normalizeTmdbQuery(originalTitle || chineseTitle);
            if (preciseQuery) {
                if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB搜索] Stage 1: 尝试查询 "${preciseQuery}" (年份: ${validYear})`);
                try {
                    const params = { query: preciseQuery, language: "zh-CN", include_adult: false };
                    if (searchMediaType === 'tv') params.first_air_date_year = validYear; else params.primary_release_year = validYear;
                    // 内部API调用依赖宿主缓存，使用主流程重试次数
                    const results = await fetchWithRetry(`/search/${searchMediaType}`, { params }, 'get', true, WidgetConfig.HTTP_MAIN_RETRIES);
                    if (results?.results?.length > 0) {
                        for (const result of results.results) {
                            const resDate = result.release_date || result.first_air_date;
                            if (resDate && resDate.startsWith(String(validYear))) {
                                let score = scoreTmdbResult(result, preciseQuery, validYear, searchMediaType, originalTitle, chineseTitle) + WidgetConfig.TMDB_SEARCH_STAGE1_YEAR_STRICT_SCORE_BOOST;
                                if (score > highestOverallScore) { highestOverallScore = score; bestOverallMatch = result; }
                            }
                        }
                        if (bestOverallMatch && WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB搜索] Stage 1 最佳匹配: ID ${bestOverallMatch.id}, 标题: "${bestOverallMatch.title||bestOverallMatch.name}", 分数: ${highestOverallScore.toFixed(2)}`);
                        if (highestOverallScore >= WidgetConfig.TMDB_SEARCH_STAGE1_HIGH_CONFIDENCE_EXIT_SCORE) {
                            if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB搜索] Stage 1 结果 (${highestOverallScore.toFixed(2)}) 已达高可信阈值，提前退出。`);
                            return bestOverallMatch; 
                        }
                    }
                } catch (e) { if (WidgetConfig.DEBUG_LOGGING) console.error(`[TMDB搜索] Stage 1 API调用错误，查询 "${preciseQuery}":`, e.message); }
            }
        }
        
        // Stage 2: 广泛搜索
        if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB搜索] Stage 2: 开始广泛搜索 (${uniqueQueries.length} 个查询)`);
        const queryPromises = uniqueQueries.map(query => async () => {
            try {
                const params = { query: query, language: "zh-CN", include_adult: false };
                const searchResults = await fetchWithRetry(`/search/${searchMediaType}`, { params }, 'get', true, WidgetConfig.HTTP_MAIN_RETRIES);
                let currentBestForQuery = null, highScoreForQuery = -Infinity;
                if (searchResults?.results?.length > 0) {
                    for (const result of searchResults.results) {
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
                    } else if (sr.status === 'rejected') {
                        if (WidgetConfig.DEBUG_LOGGING) console.error(`[TMDB搜索] Stage 2 一个查询Promise被拒绝:`, sr.reason?.message);
                        if (String(sr.reason?.message).includes("401")||String(sr.reason?.message).includes("403")) return null;
                    }
                }
            } catch (batchError) { 
                if (WidgetConfig.DEBUG_LOGGING) console.error(`[TMDB搜索] Stage 2 批处理执行错误:`, batchError.message);
                if (String(batchError.message).includes("401")||String(batchError.message).includes("403")) return null;
            }
        }

        if (bestOverallMatch && highestOverallScore >= WidgetConfig.TMDB_SEARCH_MIN_SCORE_THRESHOLD) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB搜索] 最终匹配: ID ${bestOverallMatch.id}, 标题: "${bestOverallMatch.title||bestOverallMatch.name}", 分数: ${highestOverallScore.toFixed(2)}`);
            return bestOverallMatch;
        }
        
        if (WidgetConfig.DEBUG_LOGGING) {
            const searchTarget = `BGM原名:"${originalTitle||''}" / 中文名:"${chineseTitle||''}"`;
            console.log(`[TMDB搜索] 未找到满意的TMDB匹配项 (${searchTarget.substring(0,100)}...)。最高得分:${highestOverallScore.toFixed(2)} (阈值:${WidgetConfig.TMDB_SEARCH_MIN_SCORE_THRESHOLD})`);
        }
        return null;
    });
}


// --- Bangumi页面解析工具函数 ---
function normalizeTmdbQuery(query) { if (!query || typeof query !== 'string') return ""; return query.toLowerCase().trim().replace(/[\[\]【】（）()「」『』:：\-－_,\.・]/g, ' ').replace(/\s+/g, ' ').trim();}
function scoreTmdbResult(result, query, validYear, searchMediaType, originalTitle, chineseTitle) { let currentScore = 0; const resultTitleLower = normalizeTmdbQuery(result.title || result.name); const resultOriginalTitleLower = normalizeTmdbQuery(result.original_title || result.original_name); const queryLower = normalizeTmdbQuery(query); const primaryBgmTitleLower = normalizeTmdbQuery(originalTitle || chineseTitle); if (resultTitleLower === queryLower || resultOriginalTitleLower === queryLower) { currentScore += 15; if (primaryBgmTitleLower && (resultTitleLower === primaryBgmTitleLower || resultOriginalTitleLower === primaryBgmTitleLower)) currentScore += 5; } else if (resultTitleLower.includes(queryLower) || resultOriginalTitleLower.includes(queryLower)) { currentScore += 7; if (primaryBgmTitleLower && (resultTitleLower.includes(primaryBgmTitleLower) || resultOriginalTitleLower.includes(primaryBgmTitleLower))) currentScore += 3; } else { const queryWords = queryLower.split(/\s+/).filter(w => w.length > 1); if (queryWords.length > 0) { const titleWords = new Set([...resultTitleLower.split(/\s+/), ...resultOriginalTitleLower.split(/\s+/)]); let commonWords = 0; queryWords.forEach(qw => { if (titleWords.has(qw)) commonWords++; }); currentScore += (commonWords / queryWords.length) * 6; } else { currentScore -= 2; } } if (validYear) { const resDate = result.release_date || result.first_air_date; if (resDate && resDate.startsWith(String(validYear).substring(0,4))) { const resYear = parseInt(resDate.substring(0, 4), 10); const yearDiff = Math.abs(resYear - validYear); if (yearDiff === 0) currentScore += 6; else if (yearDiff === 1) currentScore += 3; else if (yearDiff <= 2) currentScore += 1; else currentScore -= (yearDiff * 1.5); } else { currentScore -= 2; } } else { currentScore += 1; } if (result.original_language === 'ja' && (searchMediaType === 'tv' || searchMediaType === 'movie')) currentScore += 2.5; currentScore += Math.log10((result.popularity || 0) + 1) * 2.2 + Math.log10((result.vote_count || 0) + 1) * 1.2; if (result.adult) currentScore -= 10; return currentScore; }
function getInfoFromBox($, labelText) { let value = '';const listItems = $('#infobox li');for (let i = 0; i < listItems.length; i++) { const liElement = listItems.eq(i); const tipSpan = liElement.find('span.tip').first(); if (tipSpan.text().trim() === labelText) { value = liElement.clone().children('span.tip').remove().end().text().trim(); return value; } } return value; }
function parseDate(dateStr) { if (!dateStr || typeof dateStr !== 'string') return ''; dateStr = dateStr.trim(); let match; match = dateStr.match(/^(\d{4})年(\d{1,2})月(\d{1,2})日/); if (match) return `${match[1]}-${String(match[2]).padStart(2, '0')}-${String(match[3]).padStart(2, '0')}`; match = dateStr.match(/^(\d{4})年(\d{1,2})月(?!日)/); if (match) return `${match[1]}-${String(match[2]).padStart(2, '0')}-01`; match = dateStr.match(/^(\d{4})年(冬|春|夏|秋)/); if (match) { let m = '01'; if (match[2] === '春') m = '04'; else if (match[2] === '夏') m = '07'; else if (match[2] === '秋') m = '10'; return `${match[1]}-${m}-01`; } match = dateStr.match(/^(\d{4})年(?![\d月春夏秋冬])/); if (match) return `${match[1]}-01-01`; match = dateStr.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})/); if (match) return `${match[1]}-${String(match[2]).padStart(2, '0')}-${String(match[3]).padStart(2, '0')}`; match = dateStr.match(/^(\d{4})[-/](\d{1,2})(?!.*[-/])/); if (match) return `${match[1]}-${String(match[2]).padStart(2, '0')}-01`; match = dateStr.match(/^(\d{4})$/); if (match) return `${match[1]}-01-01`; if (WidgetConfig.DEBUG_LOGGING && dateStr) console.warn(`[日期解析] 无法解析日期字符串: "${dateStr}"`); return '';}
function parseBangumiListItems(htmlContent) { const $ = Widget.html.load(htmlContent); const pendingItems = []; $('ul#browserItemList li.item').each((index, element) => { const $item = $(element); let subjectId = $item.attr('id'); if (subjectId && subjectId.startsWith('item_')) { subjectId = subjectId.substring(5); } else { if(WidgetConfig.DEBUG_LOGGING) console.warn("[BGM列表解析] 无法解析条目ID:", $item.find('h3 a.l').text() || '未知条目'); return; } const titleElement = $item.find('div.inner > h3 > a.l'); const title = titleElement.text().trim(); const detailLink = titleElement.attr('href'); if (!detailLink || !detailLink.trim()) { if(WidgetConfig.DEBUG_LOGGING) console.warn(`[BGM列表解析] 条目 "${title}" (ID: ${subjectId}) 没有有效的详情链接，已跳过。`); return; } const fullDetailLink = `https://bgm.tv${detailLink}`; let listCoverUrl = $item.find('a.subjectCover img.cover').attr('src'); if (listCoverUrl && listCoverUrl.startsWith('//')) { listCoverUrl = 'https:' + listCoverUrl; } else if (!listCoverUrl) { listCoverUrl = ''; } const rating = $item.find('div.inner > p.rateInfo > small.fade').text().trim(); const infoTextFromList = $item.find('div.inner > p.info.tip').text().trim(); pendingItems.push({ id: subjectId, titleFromList: title, detailLink: fullDetailLink, coverFromList: listCoverUrl, ratingFromList: rating || "0", infoTextFromList: infoTextFromList }); }); if (WidgetConfig.DEBUG_LOGGING) console.log(`[BGM列表解析] 从列表页解析到 ${pendingItems.length} 个潜在条目。`); return pendingItems; }

// --- 条目构建与数据集成辅助函数 ---
function buildBaseItemStructure(pendingItem, detailData) { const {oTitle, cTitle, bPoster, rDate, dMTWidget, fRating} = detailData; const displayTitle = cTitle || oTitle || pendingItem.titleFromList; return { id:String(pendingItem.id), type:"link", title:displayTitle, posterPath:bPoster, backdropPath:'', releaseDate:rDate, mediaType:dMTWidget, rating:fRating, description:"", genreTitle:null, link:pendingItem.detailLink, tmdb_id:null, tmdb_overview:"", tmdb_genres:null, tmdb_tagline:"", tmdb_status:"", tmdb_original_title:"", tmdb_preferred_title:"" }; }
async function integrateTmdbDataToItem(baseItem, tmdbResult, tmdbSearchType, bgmReleaseDate, bgmRating, bgmDisplayTitle, bgmPosterPath) { if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB集成] BGM "${bgmDisplayTitle.substring(0,30)}..." -> TMDB "${(tmdbResult.title||tmdbResult.name||'').substring(0,30)}..." (ID:${tmdbResult.id})`); baseItem.type = "tmdb"; baseItem.tmdb_id = String(tmdbResult.id); baseItem.id = String(tmdbResult.id); baseItem.title = (tmdbResult.title || tmdbResult.name || bgmDisplayTitle).trim(); baseItem.posterPath = tmdbResult.poster_path || bgmPosterPath; baseItem.backdropPath = tmdbResult.backdrop_path || ''; baseItem.releaseDate = parseDate(tmdbResult.release_date || tmdbResult.first_air_date) || bgmReleaseDate; baseItem.rating = tmdbResult.vote_average ? tmdbResult.vote_average.toFixed(1) : bgmRating; baseItem.mediaType = tmdbSearchType; baseItem.description = ""; baseItem.genreTitle = null; baseItem.link = null; if (WidgetConfig.FETCH_FULL_TMDB_DETAILS) { try { const tmdbDetail = await fetchWithRetry( `/${tmdbSearchType}/${tmdbResult.id}`, { params: { language: "zh-CN", append_to_response: WidgetConfig.TMDB_APPEND_TO_RESPONSE } }, 'get', true, WidgetConfig.HTTP_MAIN_RETRIES ); if (tmdbDetail) { baseItem.tmdb_overview = tmdbDetail.overview || ""; if (tmdbDetail.genres?.length > 0) { baseItem.tmdb_genres = tmdbDetail.genres.map(g => g.name).join(', '); } baseItem.tmdb_tagline = tmdbDetail.tagline || ""; baseItem.tmdb_status = tmdbDetail.status || ""; baseItem.tmdb_original_title = tmdbDetail.original_title || tmdbDetail.original_name || ""; let bestChineseTitleFromTmdb = ''; if (tmdbDetail.translations?.translations) { const chineseTranslation = tmdbDetail.translations.translations.find( t => t.iso_639_1 === 'zh' && t.iso_3166_1 === 'CN' && t.data && (t.data.title || t.data.name) ); if (chineseTranslation) { bestChineseTitleFromTmdb = (chineseTranslation.data.title || chineseTranslation.data.name).trim(); } } baseItem.tmdb_preferred_title = bestChineseTitleFromTmdb || baseItem.title; if (bestChineseTitleFromTmdb && bestChineseTitleFromTmdb !== baseItem.title) { if(WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB详情] 更新条目主标题为TMDB中文翻译: "${bestChineseTitleFromTmdb.substring(0,30)}..." (原TMDB标题: "${baseItem.title.substring(0,30)}...")`); baseItem.title = bestChineseTitleFromTmdb; } } } catch (e) { if (WidgetConfig.DEBUG_LOGGING) console.warn(`[TMDB详情] 获取TMDB ID ${tmdbResult.id} 的完整信息失败:`, e.message); } } }

// --- 获取并处理单个Bangumi条目详情 (其计算结果由CacheUtil缓存) ---
async function fetchItemDetails(pendingItem, categoryHint) {
    if (WidgetConfig.DEBUG_LOGGING) console.log(`[BGM详情] 开始处理: "${pendingItem.titleFromList.substring(0,50)}..." (BGM ID: ${pendingItem.id})`);
    let detailHtmlResponse;
    try {
        detailHtmlResponse = await fetchWithRetry(
            pendingItem.detailLink,
            { headers: { "User-Agent": "Mozilla/5.0 FW/1.0 CCE-CreativePrefetch-Full", "Referer": "https://bgm.tv/", "Accept-Language": "zh-CN,zh;q=0.9" } },
            'get', false, WidgetConfig.HTTP_MAIN_RETRIES // 使用主流程重试次数
        );
        if (!detailHtmlResponse?.data) {
             throw new Error(`Bangumi详情页 ${pendingItem.detailLink} 响应数据为空或无效`);
        }
    } catch (htmlError) {
        console.error(`[BGM详情] 无法获取Bangumi HTML内容 (ID ${pendingItem.id}, 链接 ${pendingItem.detailLink}):`, htmlError.message);
        return null;
    }
    
    const detailHtml = detailHtmlResponse.data;

    try {
        const $ = Widget.html.load(detailHtml);
        const oTitle = $('h1.nameSingle > a').first().text().trim();
        const cTitle = getInfoFromBox($, "中文名:");
        let bPoster = $('#bangumiInfo .infobox a.thickbox.cover').attr('href') || $('#bangumiInfo .infobox img.cover').attr('src') || pendingItem.coverFromList || '';
        if (bPoster.startsWith('//')) bPoster = 'https:' + bPoster;
        if (bPoster.startsWith('https://lain.bgm.tv/pic/cover/') || bPoster.startsWith('http://lain.bgm.tv/pic/cover/')) {
            try { bPoster = new URL(bPoster).pathname; } catch (e) { /* 保持原样 */ }
        }
        let rDateStr = getInfoFromBox($, "放送开始:") || getInfoFromBox($, "上映年度:") || getInfoFromBox($, "发售日期:") || getInfoFromBox($, "发行日期:");
        let rDate = parseDate(rDateStr);
        if (!rDate && pendingItem.infoTextFromList) {
            const dateMatchInInfo = pendingItem.infoTextFromList.match(/(\d{4}年\d{1,2}月\d{1,2}日|\d{4}年\d{1,2}月|\d{4}年[春夏秋冬]|\d{4}年)/);
            if (dateMatchInInfo?.[0]) rDate = parseDate(dateMatchInInfo[0]);
        }
        const bgmMTDisplay = ($('h1.nameSingle small.grey').first().text().trim()||"").toLowerCase();
        let dMTWidget = categoryHint; let tmdbSType = '';
        if (categoryHint === 'anime' || categoryHint === 'real') {
            if (bgmMTDisplay.includes('movie') || bgmMTDisplay.includes('剧场版') || bgmMTDisplay.includes('映画')) { dMTWidget = 'movie'; }
            else { dMTWidget = 'tv'; }
            tmdbSType = dMTWidget;
        }
        const fRating = ($('#panelInterestWrapper .global_rating .number').text().trim()) || pendingItem.ratingFromList || "0";
        const yearForTmdb = rDate ? rDate.substring(0, 4) : '';
        const item = buildBaseItemStructure(pendingItem, {oTitle, cTitle, bPoster, rDate, dMTWidget, fRating});
        
        if (tmdbSType) {
            const tmdbRes = await searchTmdb(oTitle, cTitle, pendingItem.titleFromList, tmdbSType, yearForTmdb);
            if (tmdbRes?.id) {
                await integrateTmdbDataToItem(item, tmdbRes, tmdbSType, rDate, fRating, item.title, bPoster);
            } else { if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB匹配] 未能匹配BGM条目: "${item.title.substring(0,30)}..."`); }
        } else { if (WidgetConfig.DEBUG_LOGGING) console.log(`[TMDB搜索] 跳过非影视类型条目: "${item.title.substring(0,30)}..." (分类: ${categoryHint})`); }
        
        if (WidgetConfig.DEBUG_LOGGING) {
            const logItemOutput = {...item}; 
            if(logItemOutput.tmdb_overview?.length>30) logItemOutput.tmdb_overview = logItemOutput.tmdb_overview.substring(0,27)+"...";
            console.log(`[BGM详情] 处理完成: ID:${logItemOutput.id}, 类型:${logItemOutput.type}, 标题:"${logItemOutput.title.substring(0,30)}"...`);
        }
        return item;

    } catch (processingError) {
        console.error(`[BGM详情] 处理已获取的BGM HTML时发生错误 (ID ${pendingItem.id}, 标题 "${pendingItem.titleFromList.substring(0,30)}..."):`, processingError.message, processingError.stack?.substring(0,100));
        return null;
    }
}

// --- 处理整个Bangumi列表页 (包含智能预取下一页逻辑) ---
async function processBangumiPage(url, categoryHint, currentPageString) { // currentPageString 来自 params.page
    const currentPage = currentPageString ? parseInt(currentPageString, 10) : 0; // 解析为数字，无效则为0
    if (WidgetConfig.DEBUG_LOGGING) console.log(`[BGM处理] 列表页: ${url} (当前页: ${currentPage > 0 ? currentPage : '未知/1'})`);
    
    let listHtml;
    const commonHeaders = { "User-Agent": "Mozilla/5.0 FW/1.0 CCE-CreativePrefetch-Full", "Referer": `https://bgm.tv/`, "Accept-Language": "zh-CN,zh;q=0.9" };

    const prefetchedHtmlPromise = PrefetchCache.get(url);
    if (prefetchedHtmlPromise) {
        if (WidgetConfig.DEBUG_LOGGING) console.log(`[BGM处理] 使用预取缓存中的HTML Promise: ${url}`);
        try {
            listHtml = await prefetchedHtmlPromise; // 等待预取Promise完成
        } catch (e) {
            if (WidgetConfig.DEBUG_LOGGING) console.warn(`[BGM处理] 预取HTML的Promise解析失败 (${url}): ${e.message}。将尝试重新获取。`);
            listHtml = null; // 标记，以便后续重新获取
        }
    }

    if (!listHtml) {
        if (WidgetConfig.DEBUG_LOGGING && !prefetchedHtmlPromise) console.log(`[BGM处理] 未在预取缓存中找到或预取失败，正常获取HTML: ${url}`);
        try {
            const listHtmlResp = await fetchWithRetry(url, { headers: commonHeaders }, 'get', false, WidgetConfig.HTTP_MAIN_RETRIES);
            if (!listHtmlResp?.data) throw new Error("列表页响应数据为空或无效");
            listHtml = listHtmlResp.data;
        } catch (e) {
            console.error(`[BGM处理] 获取列表页 ${url} 失败:`, e.message);
            throw new Error(`请求Bangumi列表页失败: ${e.message}`);
        }
    }

    // 预取下页HTML
    if (currentPage > 0) { 
        const nextPageNum = currentPage + 1;
        let nextPageUrl;
        
        if (url.includes("page=")) {
            nextPageUrl = url.replace(/page=\d+/, `page=${nextPageNum}`);
        } else if (url.includes("?")) {
            nextPageUrl = `${url}&page=${nextPageNum}`;
        } else {
            nextPageUrl = `${url}?page=${nextPageNum}`;
        }

        if (nextPageUrl && nextPageUrl !== url) {
            if (WidgetConfig.DEBUG_LOGGING) console.log(`[BGM处理] 触发下一页 (${nextPageNum}) 的HTML预取: ${nextPageUrl}`);
            PrefetchCache.fetchAndCacheHtml(nextPageUrl, commonHeaders)
                .then(() => { if (WidgetConfig.DEBUG_LOGGING) console.log(`[预取成功完成] ${nextPageUrl}`); })
                .catch(err => { /* 预取失败的日志已在PrefetchCache.fetchAndCacheHtml中处理 */ });
        }
    }

    const pendingItems = parseBangumiListItems(listHtml);
    if (pendingItems.length === 0) { if (WidgetConfig.DEBUG_LOGGING) console.log("[BGM处理] 从HTML未解析到任何条目。"); return []; }
    if (WidgetConfig.DEBUG_LOGGING) console.log(`[BGM处理] 解析到 ${pendingItems.length} 个条目。开始并发获取详情 (最大并发: ${WidgetConfig.MAX_CONCURRENT_DETAILS_FETCH})...`);

    const results = [];
    for (let i = 0; i < pendingItems.length; i += WidgetConfig.MAX_CONCURRENT_DETAILS_FETCH) {
        const batch = pendingItems.slice(i, i + WidgetConfig.MAX_CONCURRENT_DETAILS_FETCH);
        if (WidgetConfig.DEBUG_LOGGING) console.log(`[BGM处理] 处理详情批次 ${Math.floor(i/WidgetConfig.MAX_CONCURRENT_DETAILS_FETCH)+1} (数量: ${batch.length})`);
        
        const detailPromises = batch.map(item =>
            CacheUtil.cachedOrFetch(
                'item_detail_computed_v3',
                { itemId: item.id, category: categoryHint, scriptVer: WidgetMetadata.version },
                () => fetchItemDetails(item, categoryHint)
            ).catch(e => {
                console.error(`[BGM处理] fetchItemDetails 或其缓存包装执行失败 (BGM ID: ${item.id}): `, e.message);
                return null; 
            })
        );
        
        const settledResults = await Promise.allSettled(detailPromises);
        settledResults.forEach(settledResult => {
            if (settledResult.status === 'fulfilled' && settledResult.value) {
                results.push(settledResult.value);
            } else if (settledResult.status === 'rejected') {
                 console.error(`[BGM处理] 一个条目详情Promise被拒绝(理论上应已被内部catch处理):`, settledResult.reason?.message);
            }
        });
    }
    if (WidgetConfig.DEBUG_LOGGING) console.log(`[BGM处理] 列表页处理完成。返回 ${results.length} 条有效结果.`);
    return results;
}

// --- 主要导出函数 ---
async function fetchRecentHot(params = {}) {
    const category = params.category || "anime";
    const page = params.page || "1"; // 确保page是字符串以便parseInt
    const url = `https://bgm.tv/${category}/browser/?sort=trends&page=${page}`;
    if (WidgetConfig.DEBUG_LOGGING) console.log(`[模式] 获取近期热门: 分类=${category}, 页=${page}`);
    try {
        return await processBangumiPage(url, category, page); // 传递当前页码
    } catch (error) {
        console.error(`[模式] fetchRecentHot(分类:${category}, 页码:${page}) 发生顶层错误:`, error.message);
        return [];
    }
}

async function fetchAirtimeRanking(params = {}) {
    const category = params.category || "anime";
    const year = params.year || "";
    let month = params.month || "all";
    const sort = params.sort || "rank";
    const page = params.page || "1"; // 确保page是字符串
    let url;

    if (year && /^\d{4}$/.test(year)) {
        let airtimePath = `airtime/${year}`;
        if (month && month !== "all" && /^\d{1,2}$/.test(month)) airtimePath += `/${month}`;
        url = `https://bgm.tv/${category}/browser/${airtimePath}?sort=${sort}&page=${page}`;
    } else {
        url = `https://bgm.tv/${category}/browser/?sort=${sort}&page=${page}`;
        if (year && WidgetConfig.DEBUG_LOGGING) {
            console.warn(`[模式] 时段排行提供的年份 "${year}" 格式无效。将浏览所有年份。`);
        }
    }
    if (WidgetConfig.DEBUG_LOGGING) console.log(`[模式] 获取时段排行: 分类=${category}, 年份=${year || '任意'}, 月份/季度=${month}, 排序=${sort}, 页码=${page}`);
    try {
        return await processBangumiPage(url, category, page); 
    } catch (error) {
        console.error(`[模式] fetchAirtimeRanking(分类:${category}, 年:${year}, 月:${month}, 排序:${sort}, 页:${page}) 发生顶层错误:`, error.message);
        return [];
    }
                 }
