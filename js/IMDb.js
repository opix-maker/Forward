
const GITHUB_OWNER = "opix-maker"; 
const GITHUB_REPO = "Forward";
const GITHUB_BRANCH = "main"; 
const DATA_REPO_PATH = 'imdb-data-platform/dist/database.json'; 
const DATA_URL = `https://raw.githubusercontent.com/${GITHUB_OWNER}/${GITHUB_REPO}/${GITHUB_BRANCH}/${DATA_REPO_PATH}`;
// ****************************************************************
const IMG_BASE_BACKDROP = 'https://image.tmdb.org/t/p/original'; 
const IMG_BASE_POSTER = 'https://image.tmdb.org/t/p/w500';
const ITEMS_PER_PAGE = 30;
const CURRENT_YEAR = new Date().getFullYear();
const RECENT_YEAR_THRESHOLD = CURRENT_YEAR - 1; 

console.log("[IMDb Widget Final-v1] 脚本初始化. 数据源 URL:", DATA_URL);

// --- 共享参数 ---
const pageParam = { name: "page", title: "页码", type: "page", value: "1" };
const sortOptions = [ { title: "🔥综合热度", value: "hotness_score_desc" }, { title: "👍评分", value: "vote_average_desc" }, ];
const sortParam = (defaultValue = "hotness_score_desc") => ({ name: "sort", title: "排序方式", type: "enumeration", value: defaultValue, enumOptions: sortOptions });
const mediaTypeParam = { name: "mediaType", title: "类型", type: "enumeration", value: "all", enumOptions: [{ title: "全部", value: "all" }, { title: "电影", value: "type:movie" }, { title: "剧集", value: "type:tv" }, { title: "动画", value: "type:animation"}] };
const yearOptions = [{ title: "全部", value: "all" }];
 for(let y = CURRENT_YEAR; y >= CURRENT_YEAR - 8 && y >= 1990 ; y--) { yearOptions.push({title: `${y} 年`, value: `${y}`});}
const yearEnumParam = { name: "year", title: "年份", type: "enumeration", value: "all", description:"选择特定年份", enumOptions: yearOptions };
const regionOptions = [
    { title: "全部地区", value: "all" }, { title: "华语", value: "region:chinese" }, { title: "中国大陆", value: "country:cn" }, { title: "香港", value: "country:hk" }, { title: "台湾", value: "country:tw" },
    { title: "欧美", value: "region:us-eu" }, { title: "美国", value: "country:us" }, { title: "英国", value: "country:gb" }, { title: "日韩", value: "region:east-asia"},{ title: "日本", value: "country:jp" }, { title: "韩国", value: "country:kr" },
    { title: "法国", value: "country:fr" }, { title: "德国", value: "country:de" }, { title: "加拿大", value: "country:ca" }, {title: "澳大利亚", value: "country:au"}
].sort((a,b) => a.title.localeCompare(b.title, 'zh-Hans-CN'));
const regionParamSelect =  { name: "region", title: "选择地区/语言", type: "enumeration", value: "all", enumOptions: regionOptions };
const regionParamGeneral = { name: "region", title: "地区", type: "enumeration", value: "all", enumOptions: [ { title: "全部", value: "all" }, { title: "欧美", value: "region:us-eu" }, { title: "华语", value: "region:chinese" }, { title: "日韩", value: "region:east-asia"}] };
const genreMap = [
    { title: "爱情", value: "genre:爱情" },{ title: "冒险", value: "genre:冒险" },{ title: "悬疑", value: "genre:悬疑" }, { title: "惊悚", value: "genre:惊悚" },{ title: "恐怖", value: "genre:恐怖" },{ title: "科幻", value: "genre:科幻" },
    { title: "奇幻", value: "genre:奇幻" },{ title: "动作", value: "genre:动作" },{ title: "喜剧", value: "genre:喜剧" }, { title: "剧情", value: "genre:剧情" }, { title: "历史", value: "genre:历史" },{ title: "战争", value: "genre:战争" },{ title: "犯罪", value: "genre:犯罪" },
    { title: "侦探", value: "theme:whodunit" },{ title: "谍战", value: "theme:spy" },{ title: "律政", value: "theme:courtroom" }, { title: "校园/日常", value: "theme:slice-of-life" }, { title: "武侠", value: "theme:wuxia" }, { title: "超英", value: "theme:superhero" },
];
const genreOptions = [{ title: "全部类型", value: "all" }, ...genreMap].sort((a,b) => a.title.localeCompare(b.title, 'zh-Hans-CN'));
const genreParam = { name: "genre", title: "选择类型", type: "enumeration", value: "all", enumOptions: genreOptions };
const themeOptions = [
     { title: "全部主题", value: "all" }, { title: "赛博朋克", value: "theme:cyberpunk" }, { title: "太空歌剧", value: "theme:space-opera" }, { title: "时间旅行", value: "theme:time-travel" }, { title: "末世废土", value: "theme:post-apocalyptic" }, { title: "机甲", value: "theme:mecha" },{ title: "丧尸", value: "theme:zombie" }, { title: "怪物", value: "theme:monster" }, { title: "灵异", value: "theme:ghost" }, { title: "魔法", value: "theme:magic" },{ title: "黑帮", value: "theme:gangster" }, { title: "黑色电影", value: "theme:film-noir" }, { title: "连环杀手", value: "theme:serial-killer" },{ title: "仙侠", value: "theme:xianxia" }, { title: "怪兽(Kaiju)", value: "theme:kaiju" }, { title: "异世界", value: "theme:isekai" },
].sort((a,b) => a.title.localeCompare(b.title, 'zh-Hans-CN'));
const themeParam = { name: "theme", title: "选择主题", type: "enumeration", value: "all", enumOptions: themeOptions };

// --- Widget Metadata  ---
var WidgetMetadata = {
    id: "imdb_discovery_curated_local_final",
    title: "IMDb 分类资源",
    description: "聚合IMDb/TMDB热门影视资源，提供多维度分类与榜单。",
    author: "Autism", site: `https://github.com/${GITHUB_OWNER}/${GITHUB_REPO}`, version: "1.0.1", requiredVersion: "0.0.1",
    detailCacheDuration: 3600, cacheDuration: 18000, 
    modules: [
        { title: "🆕 近期热门", description: `按综合热度浏览近两年(${RECENT_YEAR_THRESHOLD}-)的影视`, functionName: "listRecentHot", params: [ regionParamGeneral, mediaTypeParam, sortParam("hotness_score_desc"), pageParam] },
        { title: "🎭 按类型浏览", description: "按影片风格或类型(Genre)筛选", functionName: "listByGenre", params: [ genreParam, mediaTypeParam, regionParamGeneral, sortParam(), pageParam] },
        { title: "🔮 按主题浏览", description: "按特定主题(Theme)筛选", functionName: "listByTheme", params: [ themeParam, mediaTypeParam, regionParamGeneral, sortParam(), pageParam] },
        { title: "📅 按年份浏览", description: "按年份筛选", functionName: "listByYear", params: [ yearEnumParam, mediaTypeParam, regionParamGeneral, sortParam("hotness_score_desc"), pageParam] },
        { title: "🎬 电影",      description: "按地区/语言筛选电影(含动画电影)", functionName: "listMovies",    params: [ regionParamSelect, sortParam(), pageParam] },
        { title: "📺 剧集",      description: "按地区/语言筛选剧集(含动画剧集)", functionName: "listTVSeries",  params: [ regionParamSelect, sortParam(), pageParam] },
        { title: "✨ 动画",      description: "按地区/语言筛选所有动画作品", functionName: "listAnime",     params: [ regionParamSelect, sortParam(), pageParam] },
    ].map(m => ({ cacheDuration: 1800, requiresWebView: false, ...m})) 
};

// --- 地区/语言排他性规则 ---
const REGION_EXCLUSION_MAP = {
    // 选择华语时，排除欧美和日韩
    'region:chinese': ['region:us-eu', 'region:east-asia'], 'country:cn': ['region:us-eu', 'region:east-asia'],
    'country:hk': ['region:us-eu', 'region:east-asia'], 'country:tw': ['region:us-eu', 'region:east-asia'],
    // 选择日韩时，排除欧美
    'region:east-asia': ['region:us-eu'], 'country:jp': ['region:us-eu'], 'country:kr': ['region:us-eu'],
    // 选择欧美时，排除所有亚洲
    'region:us-eu': ['region:chinese', 'region:east-asia'], 'country:us': ['region:chinese', 'region:east-asia'],
    'country:gb': ['region:chinese', 'region:east-asia'], 'country:fr': ['region:chinese', 'region:east-asia'],
    'country:de': ['region:chinese', 'region:east-asia'], 'country:ca': ['region:chinese', 'region:east-asia'],
    'country:au': ['region:chinese', 'region:east-asia'],
};


let globalDatabaseCache = null; let dataFetchPromise = null; let dataTimestamp = null; let scoresCalculated = false; 
let GLOBAL_AVERAGE_RATING = 6.8; let MINIMUM_VOTES_THRESHOLD = 500;
function calculateScores(item) {
    if (!item) return; const pop = item.popularity || 0; const year = item.release_year || 1970; const R = item.vote_average || 0; const v = item.vote_count || 0; const yearDiff = Math.max(0, CURRENT_YEAR - year);
    const m = MINIMUM_VOTES_THRESHOLD; const C = GLOBAL_AVERAGE_RATING; const bayesianRating = (v / (v + m)) * R + (m / (v + m)) * C;
    item.hotness_score = Math.log10(pop + 1) * (1 / Math.sqrt(yearDiff + 2)) * bayesianRating;
}
async function fetchAndCacheGlobalData() {
    const CACHE_DURATION = 15 * 60 * 1000; 
    if (globalDatabaseCache && dataTimestamp && (Date.now() - dataTimestamp < CACHE_DURATION)) { if (!scoresCalculated) { globalDatabaseCache.forEach(calculateScores); scoresCalculated = true; } return globalDatabaseCache; }
    if (dataFetchPromise) { return await dataFetchPromise; }
    dataFetchPromise = (async () => {
        let response = null; scoresCalculated = false; 
        try {
            const bustCacheUrl = DATA_URL + '?t=' + Date.now();
            response = await Widget.http.get(bustCacheUrl, { timeout: 45000, headers: { 'Accept': 'application/json', 'Cache-Control': 'no-cache', 'User-Agent': 'ForwardWidget/IMDb-Discovery-Client/Final-v1' }});
            if (!response || typeof response.statusCode !== 'number' || response.statusCode !== 200) { const statusInfo = response ? response.statusCode : 'NO_RESPONSE'; if (statusInfo === 404) throw new Error(`HTTP 404。请检查 URL 配置。`); throw new Error(`HTTP 状态码非 200 (Status: ${statusInfo})。`); }
            if (!response.data || !Array.isArray(response.data.database)) { throw new Error(`数据格式不正确，缺少 'database' 数组。`); }
            let database = response.data.database;
            const validForStats = database.filter(i => i.vote_count > 100);
            if(validForStats.length > 0) {
                 const totalRating = validForStats.reduce((sum, item) => sum + (item.vote_average || 0), 0); GLOBAL_AVERAGE_RATING = totalRating / validForStats.length;
                 const sortedVotes = validForStats.map(i => i.vote_count).sort((a,b) => a - b); MINIMUM_VOTES_THRESHOLD = sortedVotes[Math.floor(sortedVotes.length * 0.75)] || 500;
            }
             console.log(`[IMDb Final-v1 DEBUG] 全局统计: 平均分=${GLOBAL_AVERAGE_RATING.toFixed(2)}, 投票阈值=${MINIMUM_VOTES_THRESHOLD}`);
            database.forEach(item => { const isTV = item.semantic_tags.includes('type:tv'); item.mediaType = (isTV) ? 'tv' : 'movie'; });
            globalDatabaseCache = database; dataTimestamp = Date.now();
            console.log('[IMDb Final-v1 DEBUG] 计算分数...'); globalDatabaseCache.forEach(calculateScores); scoresCalculated = true;
            return globalDatabaseCache;
        } catch (error) {
            console.error(`[IMDb Final-v1 ERROR] 异常:`, error.message || error); dataFetchPromise = null; 
            if(globalDatabaseCache) { console.warn("[IMDb Final-v1 ERROR] 使用旧缓存。"); if (!scoresCalculated) { globalDatabaseCache.forEach(calculateScores); scoresCalculated = true; } return globalDatabaseCache; }
            throw new Error("无法获取或处理数据库: " + (error.message || "未知错误")); 
        } finally { dataFetchPromise = null; }
    })();
    return await dataFetchPromise;
}
function mapToWidgetItem(item) {
    if (!item) return null; const isAnimation = item.semantic_tags.includes('type:animation'); const genres = item.semantic_tags.filter(t => t.startsWith('genre:')).map(t => t.split(':')[1]);
    return {
        id: `${item.id}`, type: "tmdb", title: item.title || 'N/A', posterPath: item.poster_path ? `${IMG_BASE_POSTER}${item.poster_path}` : null, backdropPath: item.backdrop_path ? `${IMG_BASE_BACKDROP}${item.backdrop_path}` : null,
        releaseDate: item.release_date || '', mediaType: item.mediaType || 'movie', rating: item.vote_average ? item.vote_average.toFixed(1) : '0.0', genreTitle: genres.slice(0, 3).join('/'),
        duration: item.duration || 0, durationText: '', previewUrl: '', videoUrl: '',
        link: item.imdb_id ? `https://www.imdb.com/title/${item.imdb_id}` : `https://www.themoviedb.org/${item.mediaType}/${item.id}`,
        episode: 0, description: `${isAnimation?'[动画] ':''}${item.overview || ''}`, childItems: [],
    };
}
function sortItems(items, sortRule) { const sortKey = (sortRule || 'hotness_score_desc').split('_desc')[0]; if (items.length > 0 && typeof items[0][sortKey] === 'undefined') { return; } items.sort((a, b) => { const valA = a[sortKey] || 0; const valB = b[sortKey] || 0; return valB - valA; }); }
function paginate(items, page) { const p = Math.max(1, parseInt(page || "1", 10)); const start = (p - 1) * ITEMS_PER_PAGE; return items.slice(start, start + ITEMS_PER_PAGE); }

// --- 核心处理函数 ---
async function processRequest(params = {}, preset = { tags: [], excludeTags: [], onlyRecent: false }) {
    try {
        const database = await fetchAndCacheGlobalData();
        if (!database || database.length === 0) return [];
        let requiredTags = [...(preset.tags || [])]; 
        const excludedTags = [...(preset.excludeTags || [])];
        
        if (params.mediaType && params.mediaType !== 'all') { requiredTags.push(params.mediaType); }

        const region = params.region;
        
        // --- FIX: 地区/语言排他性筛选逻辑 ---
        if (region && region !== 'all') {
            requiredTags.push(region); // 必须包含用户选择的地区
            // 查找并应用排他规则
            const exclusions = REGION_EXCLUSION_MAP[region];
            if (exclusions) {
                excludedTags.push(...exclusions);
                console.log(`[IMDb Final-v1 DEBUG] 应用排他规则 for ${region}: 排除 ${exclusions.join(', ')}`);
            }
        }
        // --- 排他性逻辑结束 ---

        ['genre', 'theme'].forEach(key => { if (params[key] && params[key] !== 'all') requiredTags.push(params[key]); });
       
        const filterYear = params.year && params.year !== 'all' ? parseInt(params.year, 10) : null;
        const checkRecent = preset.onlyRecent === true;

        const filtered = database.filter(item => {
            if (!item || !item.semantic_tags) return false;
            if (filterYear && item.release_year !== filterYear) return false;
            if (checkRecent && (item.release_year === undefined || item.release_year < RECENT_YEAR_THRESHOLD)) return false;
            // 先检查是否被排除
            if (excludedTags.length > 0 && excludedTags.some(tag => item.semantic_tags.includes(tag))) return false;
            // 再检查是否满足所有必需条件
            if (requiredTags.length > 0 && !requiredTags.every(tag => item.semantic_tags.includes(tag))) return false;
            return true;
        });
        sortItems(filtered, params.sort);
        const paginated = paginate(filtered, params.page);
        return paginated.map(mapToWidgetItem).filter(Boolean);
    } catch (error) {
        console.error("[IMDb Final-v1 ERROR] processRequest 错误:", error.message || error);
        throw new Error("处理请求失败: " + (error.message || "未知错误")); 
    }
}

// 模块函数 
async function listRecentHot(p)   { return processRequest(p, { onlyRecent: true }); }
async function listByYear(p)      { return processRequest(p, {}); }
async function listByTheme(p)     { return processRequest(p, {}); }
async function listByGenre(p)     { return processRequest(p, {}); }
async function listMovies(p)      { return processRequest(p, { tags: ['type:movie'] }); }
async function listTVSeries(p)    { return processRequest(p, { tags: ['type:tv'] }); }
async function listAnime(p)       { return processRequest(p, { tags: ['type:animation'] });}
async function loadDetail(link) { return { };}
console.log("[IMDb Widget Final-v1] Script Loaded.");
