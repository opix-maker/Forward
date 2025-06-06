/*
 * =================================================================================================
 *   Bangumi Charts Widget - Build Script v1.0.0
 * =================================================================================================
 */

import fetch from 'node-fetch';
import cheerio from 'cheerio';
import fs from 'fs/promises';

// --- 核心配置 ---
// 从环境变量中安全地获取TMDB API密钥
const TMDB_API_KEY = process.env.TMDB_API_KEY;
if (!TMDB_API_KEY) {
    console.error("错误: 未找到TMDB_API_KEY环境变量。请在GitHub Secrets中设置它。");
    process.exit(1);
}

// 之前版本的所有配置项，现在集中在这里
const WidgetConfig = {
    MAX_CONCURRENT_DETAILS_FETCH: 16,
    MAX_CONCURRENT_TMDB_SEARCHES: 8,
    HTTP_MAIN_RETRIES: 8, 
    HTTP_RETRY_DELAY: 2000,
    TMDB_SEARCH_MIN_SCORE_THRESHOLD: 6,
    MAX_TOTAL_TMDB_QUERIES_TO_PROCESS: 4,
    TMDB_ANIMATION_GENRE_ID: 16,
    BGM_BASE_URL: "https://bgm.tv",
    BGM_API_USER_AGENT: "BangumiWidgetBuilder/1.0 (+https://github.com/opix-maker/Forward)"
};

const CONSTANTS = {
    MEDIA_TYPES: { TV: "tv", MOVIE: "movie", ANIME: "anime", REAL: "real" },
    BGM_API_TYPE_MAPPING: { 2: "anime", 6: "real" },
    JS_DAY_TO_BGM_API_ID: { 0: 7, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6 }
};

// --- 构建任务定义 ---
// 在这里定义所有需要预先构建的页面
const BUILD_TASKS = {
    recentHot: [
        { category: 'anime', pages: 5 },
        { category: 'real', pages: 5 }
    ],
    airtimeRanking: [
        { category: 'anime', year: '2025', month: 'all', sort: 'collects', pages: 5 },
        { category: 'anime', year: '2025', month: 'all', sort: 'rank', pages: 5 },
        { category: 'anime', year: '2024', month: 'all', sort: 'rank', pages: 5 },
        { category: 'anime', year: '2024', month: 'all', sort: 'trends', pages: 5 },
    ],
    dailyCalendar: true // 总是构建每日放送
};

// --- HTTP和工具函数 (从旧脚本迁移并适配Node.js) ---

async function fetchWithRetry(url, options, method = 'get', isTmdb = false) {
    let attempts = 0;
    const maxRetries = WidgetConfig.HTTP_MAIN_RETRIES;
    const retryDelay = WidgetConfig.HTTP_RETRY_DELAY;
    const fullUrl = isTmdb ? `https://api.themoviedb.org/3${url}?api_key=${TMDB_API_KEY}&${options.params}` : url;

    while (attempts <= maxRetries) {
        try {
            if (attempts > 0) console.log(`  [重试 ${attempts}/${maxRetries}] GET ${fullUrl.substring(0, 100)}...`);
            const response = await fetch(fullUrl, {
                method: method,
                headers: { 'User-Agent': WidgetConfig.BGM_API_USER_AGENT, ...options.headers },
            });
            if (!response.ok) throw new Error(`HTTP错误! 状态: ${response.status}`);
            return await response.json();
        } catch (error) {
            attempts++;
            console.warn(`  [请求失败] GET ${fullUrl.substring(0, 100)}... 错误: ${error.message}`);
            if (attempts > maxRetries) throw error;
            await new Promise(resolve => setTimeout(resolve, retryDelay * attempts));
        }
    }
}

function normalizeTmdbQuery(query) { if (!query || typeof query !== 'string') return ""; return query.toLowerCase().trim().replace(/[\[\]【】（）()「」『』:：\-－_,\.・]/g, ' ').replace(/\s+/g, ' ').trim();}
function parseDate(dateStr) { if (!dateStr || typeof dateStr !== 'string') return ''; dateStr = dateStr.trim(); let match; match = dateStr.match(/^(\d{4})年(\d{1,2})月(\d{1,2})日/); if (match) return `${match[1]}-${String(match[2]).padStart(2, '0')}-${String(match[3]).padStart(2, '0')}`; match = dateStr.match(/^(\d{4})年(\d{1,2})月(?!日)/); if (match) return `${match[1]}-${String(match[2]).padStart(2, '0')}-01`; match = dateStr.match(/^(\d{4})年(冬|春|夏|秋)/); if (match) { let m = '01'; if (match[2] === '春') m = '04'; else if (match[2] === '夏') m = '07'; else if (match[2] === '秋') m = '10'; return `${match[1]}-${m}-01`; } match = dateStr.match(/^(\d{4})年(?![\d月春夏秋冬])/); if (match) return `${match[1]}-01-01`; match = dateStr.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})/); if (match) return `${match[1]}-${String(match[2]).padStart(2, '0')}-${String(match[3]).padStart(2, '0')}`; match = dateStr.match(/^(\d{4})[-/](\d{1,2})(?!.*[-/])/); if (match) return `${match[1]}-${String(match[2]).padStart(2, '0')}-01`; match = dateStr.match(/^(\d{4})$/); if (match) return `${match[1]}-01-01`; return '';}

// --- 数据处理核心逻辑 (从旧脚本迁移) ---

// TMDB评分函数
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

// TMDB搜索查询词生成
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
        queriesToProcess = queriesToProcess.slice(0, WidgetConfig.MAX_TOTAL_TMDB_QUERIES_TO_PROCESS);
    }
    return queriesToProcess;
}

// TMDB搜索核心逻辑 (全并行)
async function searchTmdb(originalTitle, chineseTitle, listTitle, searchMediaType, year) {
    let bestOverallMatch = null;
    let highestOverallScore = -Infinity;
    const validYear = year && /^\d{4}$/.test(year) ? parseInt(year, 10) : null;

    const queriesToProcess = generateTmdbSearchQueries(originalTitle, chineseTitle, listTitle);
    if (queriesToProcess.length === 0) return null;

    const queryPromises = queriesToProcess.flatMap(query => {
        const tasks = [];
        tasks.push(async () => {
            const params = new URLSearchParams({ query, language: "zh-CN", include_adult: false }).toString();
            return { response: await fetchWithRetry(`/search/${searchMediaType}`, { params }, 'get', true), query, year: null };
        });
        if (validYear) {
            tasks.push(async () => {
                const paramsObj = { query, language: "zh-CN", include_adult: false };
                if (searchMediaType === CONSTANTS.MEDIA_TYPES.TV) paramsObj.first_air_date_year = validYear;
                else paramsObj.primary_release_year = validYear;
                const params = new URLSearchParams(paramsObj).toString();
                return { response: await fetchWithRetry(`/search/${searchMediaType}`, { params }, 'get', true), query, year: validYear };
            });
        }
        return tasks;
    });

    for (let i = 0; i < queryPromises.length; i += WidgetConfig.MAX_CONCURRENT_TMDB_SEARCHES) {
        const batch = queryPromises.slice(i, i + WidgetConfig.MAX_CONCURRENT_TMDB_SEARCHES).map(p => p().catch(e => null));
        const settledResults = await Promise.allSettled(batch);

        for (const sr of settledResults) {
            if (sr.status === 'fulfilled' && sr.value) {
                const { response, query } = sr.value;
                const searchResults = response?.results;
                if (searchResults?.length > 0) {
                    for (const result of searchResults) {
                        if (searchMediaType === CONSTANTS.MEDIA_TYPES.TV && !(result.genre_ids && result.genre_ids.includes(WidgetConfig.TMDB_ANIMATION_GENRE_ID))) continue;
                        const score = scoreTmdbResult(result, query, validYear, searchMediaType, originalTitle, chineseTitle);
                        if (score > highestOverallScore) {
                            highestOverallScore = score;
                            bestOverallMatch = result;
                        }
                    }
                }
            }
        }
    }

    if (bestOverallMatch && highestOverallScore >= WidgetConfig.TMDB_SEARCH_MIN_SCORE_THRESHOLD) {
        return bestOverallMatch;
    }
    return null;
}

// 页面处理和条目增强
async function processBangumiPage(url, categoryHint) {
    console.log(`  开始处理页面: ${url}`);
    const response = await fetch(url, { headers: { 'User-Agent': WidgetConfig.BGM_API_USER_AGENT } });
    const htmlContent = await response.text();
    const $ = cheerio.load(htmlContent);
    const pendingItems = [];
    $('ul#browserItemList li.item').each((_, element) => {
        const $item = $(element);
        const titleElement = $item.find('div.inner > h3 > a.l');
        const infoTextFromList = $item.find('div.inner > p.info.tip').text().trim();
        pendingItems.push({
            titleFromList: titleElement.text().trim(),
            infoTextFromList: infoTextFromList,
        });
    });

    const results = [];
    for (let i = 0; i < pendingItems.length; i += WidgetConfig.MAX_CONCURRENT_DETAILS_FETCH) {
        const batch = pendingItems.slice(i, i + WidgetConfig.MAX_CONCURRENT_DETAILS_FETCH);
        const detailPromises = batch.map(async (item) => {
            const yearMatch = item.infoTextFromList.match(/(\d{4})/);
            const yearForTmdb = yearMatch ? yearMatch[1] : '';
            let tmdbSType = CONSTANTS.MEDIA_TYPES.TV;
            const titleLower = item.titleFromList.toLowerCase();
            const infoLower = (item.infoTextFromList || "").toLowerCase();
            if (titleLower.includes("movie") || titleLower.includes("剧场版") || titleLower.includes("映画") ||
                infoLower.includes("movie") || infoLower.includes("剧场版") || infoLower.includes("映画")) {
                tmdbSType = CONSTANTS.MEDIA_TYPES.MOVIE;
            }

            const tmdbRes = await searchTmdb(item.titleFromList, null, item.titleFromList, tmdbSType, yearForTmdb);
            if (tmdbRes?.id) {
                return {
                    id: String(tmdbRes.id),
                    type: "tmdb",
                    mediaType: tmdbSType,
                    title: (tmdbRes.title || tmdbRes.name || item.titleFromList).trim(),
                    posterPath: tmdbRes.poster_path || '',
                    backdropPath: tmdbRes.backdrop_path || '',
                    releaseDate: parseDate(tmdbRes.release_date || tmdbRes.first_air_date) || '',
                    rating: tmdbRes.vote_average ? tmdbRes.vote_average.toFixed(1) : "0",
                    description: tmdbRes.overview || item.infoTextFromList,
                };
            }
            return null; // 如果TMDB匹配失败，则不返回该条目
        });
        const settledResults = await Promise.allSettled(detailPromises);
        settledResults.forEach(sr => {
            if (sr.status === 'fulfilled' && sr.value) {
                results.push(sr.value);
            }
        });
    }
    console.log(`  页面处理完成: ${url}, 获得 ${results.length} 个有效条目。`);
    return results.filter(Boolean);
}

// 每日放送处理
async function processDailyCalendar() {
    console.log("  开始处理每日放送API...");
    const response = await fetch("https://api.bgm.tv/calendar", { headers: { 'User-Agent': WidgetConfig.BGM_API_USER_AGENT } });
    const apiData = await response.json();
    let allItems = [];
    apiData.forEach(day => {
        day.items.forEach(item => {
            allItems.push({ ...item, bgm_weekday_id: day.weekday.id });
        });
    });

    const results = [];
    for (let i = 0; i < allItems.length; i += WidgetConfig.MAX_CONCURRENT_DETAILS_FETCH) {
        const batch = allItems.slice(i, i + WidgetConfig.MAX_CONCURRENT_DETAILS_FETCH);
        const detailPromises = batch.map(async (item) => {
            const itemYear = item.air_date ? item.air_date.substring(0, 4) : '';
            const bgmCategoryHint = CONSTANTS.BGM_API_TYPE_MAPPING[item.type] || 'unknown';
            let tmdbSearchType = '';
            if (bgmCategoryHint === CONSTANTS.MEDIA_TYPES.ANIME) tmdbSearchType = CONSTANTS.MEDIA_TYPES.TV;
            else if (bgmCategoryHint === CONSTANTS.MEDIA_TYPES.REAL) tmdbSearchType = CONSTANTS.MEDIA_TYPES.MOVIE;
            
            let cover = item.images?.large || item.images?.common || "";
            if (cover.startsWith("//")) cover = "https:" + cover;

            const baseItem = {
                id: String(item.id),
                type: "link",
                mediaType: bgmCategoryHint,
                title: `${item.name_cn || item.name}`,
                posterPath: cover,
                releaseDate: item.air_date || "",
                rating: item.rating?.score ? item.rating.score.toFixed(1) : "N/A",
                description: item.summary || "",
                bgm_id: String(item.id),
                bgm_rating_total: item.rating?.total || 0,
                bgm_score: item.rating?.score || 0,
                bgm_weekday_id: item.bgm_weekday_id,
                tmdb_origin_countries: []
            };

            if (tmdbSearchType) {
                const tmdbRes = await searchTmdb(item.name, item.name_cn, item.name_cn || item.name, tmdbSearchType, itemYear);
                if (tmdbRes?.id) {
                    baseItem.id = String(tmdbRes.id);
                    baseItem.type = "tmdb";
                    baseItem.mediaType = tmdbSearchType;
                    baseItem.title = (tmdbRes.title || tmdbRes.name || baseItem.title).trim();
                    baseItem.posterPath = tmdbRes.poster_path || baseItem.posterPath;
                    baseItem.backdropPath = tmdbRes.backdrop_path || '';
                    baseItem.releaseDate = parseDate(tmdbRes.release_date || tmdbRes.first_air_date) || baseItem.releaseDate;
                    baseItem.rating = tmdbRes.vote_average ? tmdbRes.vote_average.toFixed(1) : baseItem.rating;
                    baseItem.description = tmdbRes.overview || baseItem.description;
                    baseItem.tmdb_origin_countries = tmdbRes.origin_country || [];
                }
            }
            return baseItem;
        });
        const settledResults = await Promise.allSettled(detailPromises);
        settledResults.forEach(sr => {
            if (sr.status === 'fulfilled' && sr.value) {
                results.push(sr.value);
            }
        });
    }
    console.log(`  每日放送处理完成, 获得 ${results.length} 个有效条目。`);
    return results;
}


// --- 主构建函数 ---
async function main() {
    console.log("========================================");
    console.log("  Bangumi小部件数据构建开始");
    console.log(`  开始时间: ${new Date().toISOString()}`);
    console.log("========================================");

    const finalData = {
        buildTimestamp: new Date().toISOString(),
        recentHot: {},
        airtimeRanking: {},
        dailyCalendar: {}
    };

    // 1. 构建近期热门
    console.log("\n[阶段 1/3] 构建近期热门...");
    for (const task of BUILD_TASKS.recentHot) {
        finalData.recentHot[task.category] = [];
        for (let page = 1; page <= task.pages; page++) {
            const url = `${WidgetConfig.BGM_BASE_URL}/${task.category}/browser/?sort=trends&page=${page}`;
            const pageData = await processBangumiPage(url, task.category);
            finalData.recentHot[task.category].push(pageData);
        }
    }

    // 2. 构建年度/季度榜单
    console.log("\n[阶段 2/3] 构建年度/季度榜单...");
    for (const task of BUILD_TASKS.airtimeRanking) {
        const { category, year, month, sort, pages } = task;
        if (!finalData.airtimeRanking[category]) finalData.airtimeRanking[category] = {};
        if (!finalData.airtimeRanking[category][year]) finalData.airtimeRanking[category][year] = {};
        if (!finalData.airtimeRanking[category][year][month]) finalData.airtimeRanking[category][year][month] = {};
        finalData.airtimeRanking[category][year][month][sort] = [];

        for (let page = 1; page <= pages; page++) {
            const url = `${WidgetConfig.BGM_BASE_URL}/${category}/browser/airtime/${year}/${month}?sort=${sort}&page=${page}`;
            const pageData = await processBangumiPage(url, category);
            finalData.airtimeRanking[category][year][month][sort].push(pageData);
        }
    }

    // 3. 构建每日放送
    if (BUILD_TASKS.dailyCalendar) {
        console.log("\n[阶段 3/3] 构建每日放送...");
        const calendarItems = await processDailyCalendar();
        finalData.dailyCalendar = { all_week: calendarItems };
    }

    // 4. 写入文件
    console.log("\n[完成] 所有数据构建完毕，正在写入 precomputed_data.json...");
    await fs.writeFile('precomputed_data.json', JSON.stringify(finalData, null, 2));
    console.log("文件写入成功！");
    console.log("========================================");
    console.log(`  构建结束: ${new Date().toISOString()}`);
    console.log("========================================");
}

main().catch(error => {
    console.error("\n构建过程中发生致命错误:", error);
    process.exit(1);
});
