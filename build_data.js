/*
 * =================================================================================================
 *   Bangumi Charts Widget - DATA BUILD SCRIPT (v1.2)
 * =================================================================================================
 */

const fs = require('fs').promises;
const fetch = require('node-fetch');
const cheerio = require('cheerio');

// --- 核心配置 ---
const WidgetConfig = {
    MAX_CONCURRENT_DETAILS_FETCH: 32,
    MAX_CONCURRENT_TMDB_SEARCHES: 16,
    HTTP_MAIN_RETRIES: 3,
    HTTP_RETRY_DELAY: 1500,
    BGM_BASE_URL: "https://bgm.tv",
    TMDB_SEARCH_MIN_SCORE_THRESHOLD: 6,
    MAX_TOTAL_TMDB_QUERIES_TO_PROCESS: 4,
    TMDB_ANIMATION_GENRE_ID: 16,
    TMDB_API_KEY: process.env.TMDB_API_KEY,
    BGM_API_USER_AGENT: process.env.BGM_USER_AGENT || `Bangumi-Data-Builder/1.0`
};

const CONSTANTS = {
    MEDIA_TYPES: { TV: "tv", MOVIE: "movie", ANIME: "anime", REAL: "real" },
    BGM_API_TYPE_MAPPING: { 2: "anime", 6: "real" },
};

// --- HTTP & API 工具 ---

async function http_get(url, options = {}) {
    const response = await fetch(url, {
        method: 'GET',
        headers: options.headers || {},
    });
    if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status} for ${url}`);
    }
    const contentType = response.headers.get("content-type");
    if (contentType && contentType.includes("application/json")) {
        return { data: await response.json() };
    }
    return { data: await response.text() };
}

async function tmdb_get(path, options = {}) {
    const params = new URLSearchParams(options.params || {});
    params.append('api_key', WidgetConfig.TMDB_API_KEY);
    const url = `https://api.themoviedb.org/3${path}?${params.toString()}`;
    const response = await fetch(url, {
        method: 'GET',
        headers: { 'Accept': 'application/json' },
    });
    if (!response.ok) {
        throw new Error(`TMDB API error! status: ${response.status} for ${url}`);
    }
    return response.json();
}

async function fetchWithRetry(fetchFn, ...args) {
    let attempts = 0;
    const maxRetries = WidgetConfig.HTTP_MAIN_RETRIES;
    const retryDelay = WidgetConfig.HTTP_RETRY_DELAY;
    while (attempts <= maxRetries) {
        try {
            if (attempts > 0) console.log(`  Retrying (${attempts}/${maxRetries})...`);
            return await fetchFn(...args);
        } catch (error) {
            attempts++;
            console.warn(`  Fetch error on attempt ${attempts}: ${error.message}`);
            if (attempts > maxRetries) throw error;
            await new Promise(resolve => setTimeout(resolve, retryDelay * attempts));
        }
    }
}

// --- 数据处理与解析工具 ---

function normalizeTmdbQuery(query) { if (!query || typeof query !== 'string') return ""; return query.toLowerCase().trim().replace(/[\[\]【】（）()「」『』:：\-－_,\.・]/g, ' ').replace(/\s+/g, ' ').trim();}
function parseDate(dateStr) { if (!dateStr || typeof dateStr !== 'string') return ''; dateStr = dateStr.trim(); let match; match = dateStr.match(/^(\d{4})年(\d{1,2})月(\d{1,2})日/); if (match) return `${match[1]}-${String(match[2]).padStart(2, '0')}-${String(match[3]).padStart(2, '0')}`; match = dateStr.match(/^(\d{4})年(\d{1,2})月(?!日)/); if (match) return `${match[1]}-${String(match[2]).padStart(2, '0')}-01`; match = dateStr.match(/^(\d{4})年(冬|春|夏|秋)/); if (match) { let m = '01'; if (match[2] === '春') m = '04'; else if (match[2] === '夏') m = '07'; else if (match[2] === '秋') m = '10'; return `${match[1]}-${m}-01`; } match = dateStr.match(/^(\d{4})年(?![\d月春夏秋冬])/); if (match) return `${match[1]}-01-01`; match = dateStr.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})/); if (match) return `${match[1]}-${String(match[2]).padStart(2, '0')}-${String(match[3]).padStart(2, '0')}`; match = dateStr.match(/^(\d{4})[-/](\d{1,2})(?!.*[-/])/); if (match) return `${match[1]}-${String(match[2]).padStart(2, '0')}-01`; match = dateStr.match(/^(\d{4})$/); if (match) return `${match[1]}-01-01`; return '';}

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

async function searchTmdb(originalTitle, chineseTitle, listTitle, searchMediaType, year) {
    let bestOverallMatch = null;
    let highestOverallScore = -Infinity;
    const validYear = year && /^\d{4}$/.test(year) ? parseInt(year, 10) : null;
    const queriesToProcess = generateTmdbSearchQueries(originalTitle, chineseTitle, listTitle);
    if (queriesToProcess.length === 0) return null;

    const queryPromises = queriesToProcess.flatMap(query => {
        const tasks = [];
        const params = { query, language: "zh-CN", include_adult: false };
        tasks.push(async () => ({ response: await fetchWithRetry(tmdb_get, `/search/${searchMediaType}`, { params }), query }));
        if (validYear) {
            const yearParams = { ...params };
            if (searchMediaType === CONSTANTS.MEDIA_TYPES.TV) yearParams.first_air_date_year = validYear;
            else yearParams.primary_release_year = validYear;
            tasks.push(async () => ({ response: await fetchWithRetry(tmdb_get, `/search/${searchMediaType}`, { params: yearParams }), query }));
        }
        return tasks;
    });

    for (let i = 0; i < queryPromises.length; i += WidgetConfig.MAX_CONCURRENT_TMDB_SEARCHES) {
        const batch = queryPromises.slice(i, i + WidgetConfig.MAX_CONCURRENT_TMDB_SEARCHES).map(p => p().catch(e => { console.error(`  TMDB search task failed: ${e.message}`); return null; }));
        const settledResults = await Promise.allSettled(batch);
        for (const sr of settledResults) {
            if (sr.status === 'fulfilled' && sr.value) {
                const { response, query } = sr.value;
                if (response?.results?.length > 0) {
                    for (const result of response.results) {
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
    if (bestOverallMatch && highestOverallScore >= WidgetConfig.TMDB_SEARCH_MIN_SCORE_THRESHOLD) return bestOverallMatch;
    return null;
}

function parseBangumiListItems(htmlContent) {
    const $ = cheerio.load(htmlContent);
    const pendingItems = [];
    $('ul#browserItemList li.item').each((_, element) => {
        const $item = $(element);
        let subjectId = $item.attr('id')?.substring(5);
        if (!subjectId) return;
        const titleElement = $item.find('div.inner > h3 > a.l');
        const title = titleElement.text().trim();
        const detailLink = titleElement.attr('href');
        if (!detailLink) return;
        let listCoverUrl = $item.find('a.subjectCover img.cover').attr('src');
        if (listCoverUrl?.startsWith('//')) listCoverUrl = 'https:' + listCoverUrl;
        const rating = $item.find('div.inner > p.rateInfo > small.fade').text().trim();
        const infoTextFromList = $item.find('div.inner > p.info.tip').text().trim();
        pendingItems.push({ id: subjectId, titleFromList: title, coverFromList: listCoverUrl || '', ratingFromList: rating || "0", infoTextFromList });
    });
    return pendingItems;
}

async function integrateTmdbDataToItem(baseItem, tmdbResult, tmdbSearchType) {
    baseItem.id = String(tmdbResult.id);
    baseItem.type = "tmdb";
    baseItem.mediaType = tmdbSearchType;
    baseItem.tmdb_id = String(tmdbResult.id);
    baseItem.title = (tmdbResult.title || tmdbResult.name || baseItem.title).trim();
    baseItem.posterPath = tmdbResult.poster_path ? `https://image.tmdb.org/t/p/w500${tmdbResult.poster_path}` : baseItem.posterPath;
    baseItem.backdropPath = tmdbResult.backdrop_path ? `https://image.tmdb.org/t/p/w780${tmdbResult.backdrop_path}` : '';
    baseItem.releaseDate = parseDate(tmdbResult.release_date || tmdbResult.first_air_date) || baseItem.releaseDate;
    baseItem.rating = tmdbResult.vote_average ? tmdbResult.vote_average.toFixed(1) : baseItem.rating;
    baseItem.description = tmdbResult.overview || baseItem.description;
    baseItem.tmdb_origin_countries = tmdbResult.origin_country || [];
    baseItem.tmdb_vote_count = tmdbResult.vote_count;
    baseItem.link = null;
}

async function fetchItemDetails(pendingItem, categoryHint) {
    let oTitle = pendingItem.titleFromList;
    let bPoster = pendingItem.coverFromList;
    let rDate = '';
    let fRating = pendingItem.ratingFromList || "0";
    let yearForTmdb = '';
    const yearMatch = pendingItem.infoTextFromList.match(/(\d{4})/);
    if (yearMatch) yearForTmdb = yearMatch[1];
    rDate = parseDate(pendingItem.infoTextFromList) || `${yearForTmdb}-01-01`;

    let tmdbSType = CONSTANTS.MEDIA_TYPES.TV;
    const infoLower = (pendingItem.infoTextFromList || "").toLowerCase();
    if (infoLower.includes("movie") || infoLower.includes("剧场版")) tmdbSType = CONSTANTS.MEDIA_TYPES.MOVIE;

    const item = {
        id: String(pendingItem.id), type: "link", title: oTitle, posterPath: bPoster,
        backdropPath: '', releaseDate: rDate, mediaType: categoryHint, rating: fRating,
        description: pendingItem.infoTextFromList, link: `${WidgetConfig.BGM_BASE_URL}/subject/${pendingItem.id}`,
        bgm_id: String(pendingItem.id), bgm_score: parseFloat(fRating) || 0,
    };

    const tmdbRes = await searchTmdb(oTitle, null, oTitle, tmdbSType, yearForTmdb);
    if (tmdbRes?.id) {
        await integrateTmdbDataToItem(item, tmdbRes, tmdbSType);
    }
    return item;
}

async function processBangumiPage(url, categoryHint) {
    console.log(`  Fetching list page: ${url}`);
    const listHtmlResp = await fetchWithRetry(http_get, url, { headers: { "User-Agent": WidgetConfig.BGM_API_USER_AGENT } });
    if (!listHtmlResp?.data) throw new Error("List page response empty");
    const pendingItems = parseBangumiListItems(listHtmlResp.data);
    console.log(`  Parsed ${pendingItems.length} items. Processing details...`);

    const results = [];
    for (let i = 0; i < pendingItems.length; i += WidgetConfig.MAX_CONCURRENT_DETAILS_FETCH) {
        const batch = pendingItems.slice(i, i + WidgetConfig.MAX_CONCURRENT_DETAILS_FETCH);
        const detailPromises = batch.map(item => fetchItemDetails(item, categoryHint).catch(e => {
            console.error(`  Failed to process item BGM ID ${item.id}: ${e.message}`);
            return null;
        }));
        const settledResults = await Promise.all(detailPromises);
        results.push(...settledResults.filter(Boolean));
    }
    return results;
}

// --- 构建任务定义 ---

async function buildRecentHot(category, totalPages) {
    const results = [];
    for (let page = 1; page <= totalPages; page++) {
        const url = `${WidgetConfig.BGM_BASE_URL}/${category}/browser/?sort=trends&page=${page}`;
        const pageItems = await processBangumiPage(url, category);
        results.push(pageItems);
    }
    return results;
}

async function buildAirtimeRanking(category, year, month, sort, totalPages) {
    const results = [];
    for (let page = 1; page <= totalPages; page++) {
        let url;
        if (year) {
            let airtimePath = `airtime/${year}`;
            if (month && month !== "all") airtimePath += `/${month}`;
            url = `${WidgetConfig.BGM_BASE_URL}/${category}/browser/${airtimePath}?sort=${sort}&page=${page}`;
        } else {
            url = `${WidgetConfig.BGM_BASE_URL}/${category}/browser/?sort=${sort}&page=${page}`;
        }
        const pageItems = await processBangumiPage(url, category);
        results.push(pageItems);
    }
    return results;
}

async function buildDailyCalendar() {
    console.log("Building Daily Calendar data...");
    const apiUrl = `https://api.bgm.tv/calendar`;
    const apiResponse = await fetchWithRetry(http_get, apiUrl, { headers: { "User-Agent": WidgetConfig.BGM_API_USER_AGENT } });
    if (!Array.isArray(apiResponse.data)) throw new Error("Calendar API response not an array");

    const allItems = [];
    apiResponse.data.forEach(dayData => {
        if (dayData.items) {
            dayData.items.forEach(item => {
                item.bgm_weekday_id = dayData.weekday?.id;
                allItems.push(item);
            });
        }
    });

    const enhancedItems = [];
    for (let i = 0; i < allItems.length; i += WidgetConfig.MAX_CONCURRENT_DETAILS_FETCH) {
        const batch = allItems.slice(i, i + WidgetConfig.MAX_CONCURRENT_DETAILS_FETCH);
        const promises = batch.map(async (item) => {
            const bgmCategoryHint = CONSTANTS.BGM_API_TYPE_MAPPING[item.type] || 'unknown';
            let tmdbSearchType = '';
            if (bgmCategoryHint === 'anime') tmdbSearchType = 'tv';
            else if (bgmCategoryHint === 'real') tmdbSearchType = 'movie';

            const baseItem = {
                id: String(item.id), type: "link", title: item.name_cn || item.name,
                posterPath: item.images?.large?.startsWith('//') ? 'https:' + item.images.large : item.images?.large,
                releaseDate: item.air_date, mediaType: bgmCategoryHint, rating: item.rating?.score?.toFixed(1) || "N/A",
                description: `[${item.weekday?.cn || ''}] ${item.summary || ''}`.trim(),
                link: item.url, bgm_id: String(item.id), bgm_score: item.rating?.score || 0,
                bgm_rating_total: item.rating?.total || 0, bgm_weekday_id: item.bgm_weekday_id
            };

            if (tmdbSearchType) {
                const tmdbRes = await searchTmdb(item.name, item.name_cn, item.name, tmdbSearchType, item.air_date?.substring(0, 4));
                if (tmdbRes?.id) {
                    await integrateTmdbDataToItem(baseItem, tmdbRes, tmdbSearchType);
                }
            }
            return baseItem;
        });
        const settled = await Promise.all(promises);
        enhancedItems.push(...settled.filter(Boolean));
    }
    console.log("Finished building Daily Calendar data.");
    return enhancedItems;
}


// --- 主执行函数 ---
async function main() {
    if (!WidgetConfig.TMDB_API_KEY) {
        throw new Error("TMDB_API_KEY environment variable is not set!");
    }
    console.log("Starting data build process...");
    const startTime = Date.now();

    const finalData = {
        buildTimestamp: new Date().toISOString(),
        recentHot: {},
        airtimeRanking: {},
        dailyCalendar: {}
    };

    try {
        console.log("\n[1/3] Building Recent Hot...");
        finalData.recentHot.anime = await buildRecentHot('anime', 5);
        finalData.recentHot.real = await buildRecentHot('real', 2);

        console.log("\n[2/3] Building Airtime Rankings...");
        const yearsToBuild = ["2025", "2024"];
        const sortsToBuild = ["collects", "rank", "trends"];
        const monthsToBuild = ["all", "1", "4", "7", "10"]; // **核心修复**
        
        finalData.airtimeRanking.anime = {};
        for (const year of yearsToBuild) {
            finalData.airtimeRanking.anime[year] = {};
            for (const month of monthsToBuild) {
                finalData.airtimeRanking.anime[year][month] = {};
                for (const sort of sortsToBuild) {
                    console.log(`- Building Anime, Year: ${year}, Month: ${month}, Sort: ${sort}`);
                    finalData.airtimeRanking.anime[year][month][sort] = await buildAirtimeRanking('anime', year, month, sort, 5);
                }
            }
        }

        console.log("\n[3/3] Building Daily Calendar...");
        finalData.dailyCalendar.all_week = await buildDailyCalendar();

        await fs.writeFile('recent_data.json', JSON.stringify(finalData, null, 2));

        const duration = (Date.now() - startTime) / 1000;
        console.log(`\nBuild process finished in ${duration.toFixed(2)} seconds.`);
        console.log("`recent_data.json` has been successfully generated.");

    } catch (error) {
        console.error("\nFATAL ERROR during build process:", error);
        process.exit(1);
    }
}

main();
