const GITHUB_OWNER = "opix-maker"; 
const GITHUB_REPO = "Forward";
const GITHUB_BRANCH = "main"; 
const DATA_REPO_PATH = 'imdb-data-platform/dist/database.json'; 
const DATA_URL = `https://raw.githubusercontent.com/${GITHUB_OWNER}/${GITHUB_REPO}/${GITHUB_BRANCH}/${DATA_REPO_PATH}`;


const IMG_BASE_BACKDROP = 'https://image.tmdb.org/t/p/original'; 
const IMG_BASE_POSTER = 'https://image.tmdb.org/t/p/w500';
const ITEMS_PER_PAGE = 30;
const CURRENT_YEAR = new Date().getFullYear();
const RECENT_YEAR_THRESHOLD = CURRENT_YEAR - 1; 

console.log("[IMDb Widget  Final] 脚本初始化. 数据源 URL:", DATA_URL);

// --- 共享参数 ---
const pageParam = { name: "page", title: "页码", type: "page", value: "1" };
const sortOptions = [ { title: "🔥综合热度", value: "hotness_score_desc" }, { title: "👍评分", value: "vote_average_desc" }, ];
const sortParam = (defaultValue = "hotness_score_desc") => ({ name: "sort", title: "排序方式", type: "enumeration", value: defaultValue, enumOptions: sortOptions });
const mediaTypeParam = { name: "mediaType", title: "类型", type: "enumeration", value: "all", enumOptions: [{ title: "全部", value: "all" }, { title: "电影", value: "type:movie" }, { title: "剧集()", value: "type:tv_live" }, { title: "动画", value: "type:animation"}] };
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

// --- Widget Metadata ---
var WidgetMetadata = {
    id: "imdb_discovery_curated_local__final",
    title: "IMDb 精选浏览 (Local Final)",
    description: "基于 GitHub Action 数据的策展式浏览列表。",
    author: "Autism", site: `https://github.com/${GITHUB_OWNER}/${GITHUB_REPO}`, version: "1.0.0", requiredVersion: "0.0.1",
    detailCacheDuration: 3600, cacheDuration: 18000, 
    modules: [
        { title: "🆕 近期热门", description: `年份>=${RECENT_YEAR_THRESHOLD}`, functionName: "listRecentHot", params: [ regionParamGeneral, mediaTypeParam, sortParam("hotness_score_desc"), pageParam] },
        { title: "🎭 按类型浏览", description: "按影片风格/类型(Genre)浏览", functionName: "listByGenre", params: [ genreParam, mediaTypeParam, regionParamGeneral, sortParam(), pageParam] },
        { title: "🔮 按主题浏览", description: "按特定主题(Theme)浏览", functionName: "listByTheme", params: [ themeParam, mediaTypeParam, regionParamGeneral, sortParam(), pageParam] },
        { title: "📅 按年份浏览", description: "按年份、地区、类型查看", functionName: "listByYear", params: [ yearEnumParam, mediaTypeParam, regionParamGeneral, sortParam("hotness_score_desc"), pageParam] },
        { title: "🎬 电影",      description: "按地区/语言筛选电影", functionName: "listMovies",    params: [ regionParamSelect, sortParam(), pageParam] },
        { title: "📺 剧集", description: "按地区/语言筛选剧集", functionName: "listTVSeries",  params: [ regionParamSelect, sortParam(), pageParam] },
        { title: "✨ 动画",     description: "按地区/语言筛选动画", functionName: "listAnime",     params: [ regionParamSelect, sortParam(), pageParam] },
    ].map(m => ({ cacheDuration: 1800, requiresWebView: false, ...m})) 
};

// --- Core Logic ---
let globalDatabaseCache = null; let dataFetchPromise = null; let dataTimestamp = null; let scoresCalculated = false; 

let GLOBAL_AVERAGE_RATING = 6.8; 
let MINIMUM_VOTES_THRESHOLD = 500; // 贝叶斯平均所需最低票数 (m)


function calculateScores(item) {
    if (!item) return;
    
    // 基础数据
    const pop = item.popularity || 0;
    const year = item.release_year || 1970; 
    const R = item.vote_average || 0; // 单项评分
    const v = item.vote_count || 0;   // 单项票数

    // 1. 平滑流行度 (log)
    const popFactor = Math.log10(pop + 1);

    // 2. 年份衰减 (平滑反比)
    const yearDiff = Math.max(0, CURRENT_YEAR - year);
    const yearDecay = 1 / Math.sqrt(yearDiff + 2);

    // 3. 贝叶斯平均评分 (WR)
    const m = MINIMUM_VOTES_THRESHOLD;
    const C = GLOBAL_AVERAGE_RATING;
    const bayesianRating = (v / (v + m)) * R + (m / (v + m)) * C;
    
    // 4. 票数权重因子 (平滑)
    const voteCountFactor = Math.log10(v + 1);

    // 最终热度分 = 平滑流行度 * 年份衰减 * 贝叶斯评分 * 票数权重
    item.hotness_score = popFactor * yearDecay * bayesianRating * voteCountFactor;
}

async function fetchAndCacheGlobalData() {
     const CACHE_DURATION = 15 * 60 * 1000; 
    if (globalDatabaseCache && dataTimestamp && (Date.now() - dataTimestamp < CACHE_DURATION)) {
       console.log('[IMDb Widget Final DEBUG] 使用内存缓存');
        if (!scoresCalculated) { console.log('[IMDb Widget Final DEBUG] 计算缓存分...'); globalDatabaseCache.forEach(calculateScores); scoresCalculated = true; }
       return globalDatabaseCache;
   }
   if (dataFetchPromise) { console.log('[IMDb Widget Final DEBUG] 等待请求...'); return await dataFetchPromise; }
   dataFetchPromise = (async () => {
        let response = null; scoresCalculated = false; 
       try {
           const bustCacheUrl = DATA_URL + '?t=' + Date.now();
           console.log(`[IMDb Widget Final DEBUG] URL: ${bustCacheUrl}`);
           response = await Widget.http.get(bustCacheUrl, { timeout: 45000, headers: { 'Accept': 'application/json', 'Cache-Control': 'no-cache', 'User-Agent': 'ForwardWidget/IMDb-Discovery-Client/13.0' }});
           console.log("[IMDb Widget Final DEBUG] Status:", (response ? response.statusCode: 'NO'), "Size:", (response && response.data ? JSON.stringify(response.data).length : 0));
            if (!response) throw new Error("网络错误，未收到响应。");
            if (typeof response.statusCode === 'undefined' || response.statusCode !== 200) {
                 const statusInfo = typeof response.statusCode !== 'undefined' ? response.statusCode : 'UNDEFINED';
                 if (statusInfo === 404) throw new Error(`HTTP 404 Not Found。请检查 URL配置。`);
                 throw new Error(`HTTP 状态码非 200 (Status: ${statusInfo})。`);
            }
           if (!response.data || typeof response.data !== 'object' || !response.data.database || !Array.isArray(response.data.database) ) {
                 const detailError = !response.data ? "data为空" : typeof response.data !== 'object' ? `data非对象` : !response.data.database ? "缺database" : "database非数组";
                throw new Error(`数据格式不正确: ${detailError}。`);
           }
           let database = response.data.database;
           
            // 预处理 mediaType
            database.forEach(item => {
                const isAnimation = item.semantic_tags.includes('type:animation'); const isTV = item.semantic_tags.includes('type:tv');
                item.mediaType = (isAnimation && isTV) ? 'tv' : (isAnimation && !isTV) ? 'movie' : (!isAnimation && isTV) ? 'tv' : 'movie';
            });
            
           globalDatabaseCache = database; dataTimestamp = Date.now();
           console.log('[IMDb Widget Final DEBUG] 计算分数...'); globalDatabaseCache.forEach(calculateScores); scoresCalculated = true;
           console.log(`[IMDb Widget Final DEBUG] 数据和分数完成，共 ${globalDatabaseCache.length} 条, 构建于: ${response.data.buildTimestamp || 'N/A'}`);
           return globalDatabaseCache;
       } catch (error) {
           console.error(`[IMDb Widget Final ERROR] 异常:`, error.message || error, "Status:", (response ? response.statusCode: 'NO'));
           dataFetchPromise = null; 
           if(globalDatabaseCache) { console.warn("[IMDb Widget Final ERROR] 使用旧缓存。");
                 if (!scoresCalculated) { globalDatabaseCache.forEach(calculateScores); scoresCalculated = true; }
                return globalDatabaseCache; 
           }
           throw new Error("无法获取或处理数据库: " + (error.message || "未知错误")); 
       } finally { dataFetchPromise = null; }
   })();
   return await dataFetchPromise;
}


function mapToWidgetItem(item) {
    if (!item) return null;
    

    const result = { ...item }; 


    const mediaType = item.mediaType || 'movie';
    const posterUrl = item.poster_path ? `${IMG_BASE_POSTER}${item.poster_path}` : null;
    const backdropUrl = item.backdrop_path ? `${IMG_BASE_BACKDROP}${item.backdrop_path}` : null; // 使用原始尺寸

    result.type = "tmdb";
    result.mediaType = mediaType;
    result.posterPath = posterUrl;
    
    result.backdropPath = backdropUrl;
    result.coverUrl = posterUrl; 

    result.description = item.overview || '';
    

    if (mediaType === 'tv') {
        result.episode = item.id; 
    }

    return result;
}


 function sortItems(items, sortRule) {
    const sortKey = (sortRule || 'hotness_score_desc').split('_desc')[0];
    if (items.length > 0 && typeof items[0][sortKey] === 'undefined' ) { console.warn(`[IMDb Widget Final WARN] 排序键 ${sortKey} 不存在`); return; }
    items.sort((a, b) => { const valA = a[sortKey] || 0; const valB = b[sortKey] || 0; return valB - valA; });
 }
 function paginate(items, page) {
    const p = Math.max(1, parseInt(page || "1", 10)); const start = (p - 1) * ITEMS_PER_PAGE;
    return items.slice(start, start + ITEMS_PER_PAGE);
 }
async function processRequest(params = {}, preset = { tags: [], excludeTags: [], onlyRecent: false, year: undefined }) {
      preset.tags = preset.tags || []; preset.excludeTags = preset.excludeTags || [];
     try {
        const database = await fetchAndCacheGlobalData();
        if (!database || database.length === 0) return [];
        let requiredTags = [...preset.tags]; const excludedTags = [...preset.excludeTags];
        if (params.mediaType === 'type:tv_live') { requiredTags.push('type:tv'); excludedTags.push('type:animation'); } 
        else if (params.mediaType && params.mediaType !== 'all') { requiredTags.push(params.mediaType); }
        const region = params.region; const isAnimeModule = preset.tags.includes('type:animation');
        if (isAnimeModule) {
             requiredTags = requiredTags.filter(t => t !== 'type:animation');
             if (region === 'country:jp' || region === 'region:east-asia') requiredTags.push('category:jp_anime');
             else if (region === 'country:cn' || region === 'region:chinese' || region === 'country:hk' || region === 'country:tw' ) requiredTags.push('category:cn_anime');
             else { requiredTags.push('type:animation'); if(region && region !== 'all') requiredTags.push(region); }
        } else { if(region && region !== 'all') requiredTags.push(region); }
        ['genre', 'theme'].forEach(key => { if (params[key] && params[key] !== 'all') requiredTags.push(params[key]); });
        const filterYear = params.year && params.year !== 'all' ? parseInt(params.year, 10) : (preset.year ? parseInt(preset.year, 10) : null) ;
        const checkRecent = preset.onlyRecent === true;
        const filtered = database.filter(item => {
            if (!item || !item.semantic_tags) return false;
            if (filterYear && item.release_year !== filterYear) return false;
            if (checkRecent && (item.release_year === undefined || item.release_year < RECENT_YEAR_THRESHOLD)) return false;
            if (excludedTags.length > 0 && excludedTags.some(tag => item.semantic_tags.includes(tag))) return false;
            if (requiredTags.length > 0 && !requiredTags.every(tag => item.semantic_tags.includes(tag))) return false;
            return true;
        });
        sortItems(filtered, params.sort);
        const paginated = paginate(filtered, params.page);
        return paginated.map(mapToWidgetItem).filter(Boolean);
      } catch (error) {
        console.error("[IMDb Widget Final ERROR] processRequest 错误:", error.message || error, "Preset:", preset, "Params:", params);
        throw new Error("处理请求失败: " + (error.message || "未知错误")); 
     }
}
async function listRecentHot(p)   { return processRequest(p, { onlyRecent: true }); }
async function listByYear(p)      { return processRequest(p, {}); }
async function listByTheme(p)     { return processRequest(p, {}); }
async function listByGenre(p)     { return processRequest(p, {}); }
async function listMovies(p)      { return processRequest(p, { tags: ['type:movie'] }); }
async function listTVSeries(p)    { return processRequest(p, { tags: ['type:tv'], excludeTags: ['type:animation'] }); }
async function listAnime(p)       { return processRequest(p, { tags: ['type:animation'] });}
async function loadDetail(link) { console.log("[IMDb Widget Final] loadDetail for: " + link); return { };}
console.log("[IMDb Widget Final] Script Loaded.");
