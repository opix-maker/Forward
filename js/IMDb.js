const GITHUB_OWNER = "opix-maker";
const GITHUB_REPO = "Forward";
const GITHUB_BRANCH = "main";
const BASE_DATA_URL = `https://raw.githubusercontent.com/${GITHUB_OWNER}/${GITHUB_REPO}/${GITHUB_BRANCH}/imdb-data-platform/dist`;
const IMG_BASE_POSTER = 'https://image.tmdb.org/t/p/w500';
const IMG_BASE_BACKDROP = 'https://image.tmdb.org/t/p/w780'; 
const ITEMS_PER_PAGE = 30;
const CURRENT_YEAR = new Date().getFullYear();
const DEBUG_LOG = true; // 设置 true 开启详细调试日志，用于排查数据源问题

console.log(`[IMDb-v1] 脚本初始化 v1.0.3`);

// --- 辅助函数 ---
 function buildImageUrl(baseUrl, path) {
     if (!path || typeof path !== 'string') { return null; }
     if (path.startsWith('http://') || path.startsWith('https://')) { return path; }
      const cleanPath = path.startsWith('/') ? path : '/' + path;
      return baseUrl + cleanPath;
 }

 function processEnumOptions(options, allValue = "all", allTitle = "全部", allLast = false) {
    let processed = [...options];
    const allIndex = processed.findIndex(opt => opt.value === allValue);
    let allItem = null;
    if (allIndex > -1) {
       allItem = processed.splice(allIndex, 1)[0];
       allItem.title = allTitle; 
    } else {
       allItem = { title: allTitle, value: allValue };
    }
     if(options.length > 0 && options.some(opt => /^\d{4}$/.test(opt.value))){
          processed.sort((a, b) => parseInt(b.value) - parseInt(a.value)); // 年份降序
     } else {
        processed.sort((a, b) => a.title.localeCompare(b.title, 'zh-Hans-CN'));
     }
    if (allLast) {
        processed.push(allItem);
    } else {
        processed.unshift(allItem);
    }
   return processed;
}

// --- 参数定义 ---
const pageParam = { name: "page", title: "页码", type: "page", value: "1" };
const sortOptions = [
    { title: "🔥综合热度", value: "hs_desc" }, { title: "👍评分", value: "r_desc" }, { title: "默认排序", value: "d_desc" }
];
const sortParam = (defaultValue = "hs_desc") => ({ name: "sort", title: "排序方式", type: "enumeration", value: defaultValue, enumOptions: sortOptions });

// 年份: 默认今年, all last, 降序
const yearOptionsRaw = [];
for(let y = CURRENT_YEAR; y >= 1990 ; y--) {
     yearOptionsRaw.push({title: `${y} 年`, value: String(y)});
}
const yearEnumParam = { name: "year", title: "年份", type: "enumeration", value: String(CURRENT_YEAR), description:"选择特定年份", enumOptions: processEnumOptions(yearOptionsRaw, "all", "全部年份", true) }; 

// 地区: 精简列表
const regionOptionsRefined = [
    { title: "中国大陆", value: "country:cn" }, { title: "美国", value: "country:us" }, { title: "英国", value: "country:gb" }, 
    { title: "日本", value: "country:jp" }, { title: "韩国", value: "country:kr" }, { title: "欧美", value: "region:us-eu" },
    { title: "香港", value: "country:hk" }, { title: "台湾", value: "country:tw" },  
 ];
// 电影/剧集/动画: 默认all, all first
const regionParamSelect =  { name: "region", title: "选择地区/语言", type: "enumeration", value: "all", enumOptions: processEnumOptions(regionOptionsRefined, "all", "全部地区", false)};
// 热门/分类/年份: 默认all, all last
const regionFilterParam = { name: "region", title: "选择地区/语言", type: "enumeration", value: "all", enumOptions: processEnumOptions(regionOptionsRefined, "all", "全部地区", true)};

// 分类/主题: 默认爱情, all last
const genreMap = [
 { title: "爱情", value: "genre:爱情" },{ title: "冒险", value: "genre:冒险" },{ title: "悬疑", value: "genre:悬疑" }, { title: "惊悚", value: "genre:惊悚" },{ title: "恐怖", value: "genre:恐怖" },{ title: "科幻", value: "genre:科幻" },
 { title: "奇幻", value: "genre:奇幻" },{ title: "动作", value: "genre:动作" },{ title: "喜剧", value: "genre:喜剧" }, { title: "剧情", value: "genre:剧情" }, { title: "历史", value: "genre:历史" },{ title: "战争", value: "genre:战争" },{ title: "犯罪", value: "genre:犯罪" },
];
const themeOptionsRaw = [
 { title: "赛博朋克", value: "theme:cyberpunk" }, { title: "太空歌剧", value: "theme:space-opera" }, { title: "时间旅行", value: "theme:time-travel" }, { title: "末世废土", value: "theme:post-apocalyptic" }, { title: "机甲", value: "theme:mecha" },{ title: "丧尸", value: "theme:zombie" }, { title: "怪物", value: "theme:monster" }, { title: "灵异", value: "theme:ghost" }, { title: "魔法", value: "theme:magic" },{ title: "黑帮", value: "theme:gangster" }, { title: "黑色电影", value: "theme:film-noir" }, { title: "连环杀手", value: "theme:serial-killer" },{ title: "仙侠", value: "theme:xianxia" }, { title: "怪兽(Kaiju)", value: "theme:kaiju" }, { title: "异世界", value: "theme:isekai" },
  { title: "侦探推理", value: "theme:whodunit" },{ title: "谍战", value: "theme:spy" },{ title: "律政", value: "theme:courtroom" }, { title: "校园/日常", value: "theme:slice-of-life" }, { title: "武侠", value: "theme:wuxia" }, { title: "超级英雄", value: "theme:superhero" }
];
const allCategoryOptions = [...genreMap, ...themeOptionsRaw];
const categoryParam = { name: "category", title: "选择分类/主题", type: "enumeration", value: "genre:爱情", enumOptions: processEnumOptions(allCategoryOptions, "all", "全部分类/主题", true) }; 

// 内容分类: 默认all, all first, 固定顺序
 const contentTypeParam = { 
    name: "contentType", title: "内容分类", type: "enumeration", value: "all", 
    enumOptions: [
       {title:"🔥全部类型", value:"all"}, {title:"🎬电影", value:"movie"}, 
       {title:"📺剧集", value:"tv"}, {title:"✨动画",value:"anime"}
     ]
 };

// --- 元数据 ---
var WidgetMetadata = {
    id: "imdb_discovery_final_v1", // 修改ID确保APP刷新
    title: "IMDb 分类资源 v1",
    description: "聚合 IMDb 热门影视资源",
    author: "Autism",
    site: "https://github.com/opix-maker/Forward",
    version: "1.0.3",
    requiredVersion: "0.0.1",
    detailCacheDuration: 36000,
    cacheDuration: 360000, // 5100hours
    modules: [
        { title: "🆕 近期热门",   functionName: "listRecentHot",   params: [contentTypeParam, regionFilterParam, sortParam("hs_desc"), pageParam], cacheDuration: 18000, requiresWebView: false },
        { title: "🎭 分类/主题", functionName: "listByCategory",  params: [categoryParam, contentTypeParam, regionFilterParam, sortParam(), pageParam], cacheDuration: 18000, requiresWebView: false },
        { title: "📅 按年份浏览", functionName: "listByYear",      params: [yearEnumParam, contentTypeParam, regionFilterParam, sortParam("d_desc"), pageParam], cacheDuration: 18000, requiresWebView: false },
        { title: "🎬 电影",       functionName: "listMovies",      params: [regionParamSelect, sortParam(), pageParam], cacheDuration: 18000, requiresWebView: false },
        { title: "📺 剧集",       functionName: "listTVSeries",    params: [regionParamSelect, sortParam(), pageParam], cacheDuration: 18000, requiresWebView: false },
        { title: "✨ 动画",       functionName: "listAnime",       params: [regionParamSelect, sortParam(), pageParam], cacheDuration: 18000, requiresWebView: false },
   ]
};
if(DEBUG_LOG) console.log(`[IMDb-v1 DEBUG] Metadata Defaults: Year=${yearEnumParam.value}, Cat=${categoryParam.value}, Type=${contentTypeParam.value}, RegionF=${regionFilterParam.value}, RegionS=${regionParamSelect.value}`);

// --- 缓存 ---
let cachedData = {};
let animeIdCache = null; 
let masterDataCache = null;
const cachedRegionIdSets = {};

// --- 核心数据获取 ---
 function getCacheBuster() {
    return Math.floor(Date.now() / (1000 * 60 * 30)); // 30 mins
 }

// --- 修改后的代码 (After) - 请复制并替换 ---
async function fetchShard(shardPath) {
    if (!shardPath || typeof shardPath !== 'string' || !shardPath.endsWith('.json')) {
       return [];
     }

    const rawUrl = `${BASE_DATA_URL}/${shardPath}?cache_buster=${getCacheBuster()}`;


    const encodedUrl = encodeURI(rawUrl);

    if (cachedData[encodedUrl]) { 
        if(DEBUG_LOG) console.log(`[IMDb-v1 DEBUG] Cache HIT: ${shardPath}`);
        return cachedData[encodedUrl]; 
    }

    if(DEBUG_LOG) console.log(`[IMDb-v1 DEBUG] Fetching: ${encodedUrl}`);
    let response;
    try {
        // 使用编码后的 encodedUrl 发起网络请求
        response = await Widget.http.get(encodedUrl, { timeout: 35000, headers: {'User-Agent': 'ForwardWidget/IMDb-v1'} }); 
    } catch (e) { 

        console.error(`[IMDb-v1 ERROR] 网络请求失败 ${encodedUrl}: ${e.message}`); 
        throw new Error(`网络请求失败: ${e.message || '未知网络错误'}`);
    }

    if (!response || typeof response.statusCode !== 'number' || response.statusCode !== 200 || !response.data ) {
       console.error(`[IMDb-v1 ERROR] 获取数据响应异常. Status: ${response ? response.statusCode : 'N/A'}, URL: ${encodedUrl}`);
        throw new Error(`获取数据失败 (Status: ${response ? response.statusCode : 'N/A'})`);
    }

    const data = Array.isArray(response.data) ? response.data : [];
    cachedData[encodedUrl] = data;
    return data;
}
async function getAnimeIds() {
    if (animeIdCache !== null) return animeIdCache;
    try {
       const allAnimeData = await fetchShard('anime/all.json');
       animeIdCache = new Set(allAnimeData.map(item => item.id));
       if(DEBUG_LOG) console.log(`[IMDb-v1 DEBUG] Cached ${animeIdCache.size} anime IDs.`);
       return animeIdCache;
    } catch(e) {
        console.error("[IMDb-v1 ERROR] Failed to fetch anime IDs:", e);
        animeIdCache = new Set(); 
        return animeIdCache; 
    }
}

async function getMasterData() {
     if (masterDataCache !== null) return masterDataCache;
      try {
          if(DEBUG_LOG) console.log("[IMDb-v1 DEBUG] Building master data cache...");
         const [movies, tv, anime] = await Promise.all([
              fetchShard('movies/all.json'), fetchShard('tvseries/all.json'), fetchShard('anime/all.json')
         ]);
          const uniqueIds = new Set();
          masterDataCache = [...movies, ...tv, ...anime].filter(item => {
             if (uniqueIds.has(item.id)) return false;
             uniqueIds.add(item.id);
             return true;
           }); // 简单去重
           if(DEBUG_LOG) console.log(`[IMDb-v1 DEBUG] Master data cached: ${masterDataCache.length} items.`);
          return masterDataCache;
      } catch(e) {
           console.error("[IMDb-v1 ERROR] Failed to fetch master data:", e);
           masterDataCache = []; 
           return [];
      }
}

async function getRegionFilteredIds(regionFilter, typeFilter) {
    if(!regionFilter || regionFilter === 'all') return null; 
    const cacheKey = `${regionFilter}|${typeFilter}`;
    if(cachedRegionIdSets[cacheKey]) return cachedRegionIdSets[cacheKey];

     const regionIds = new Set();
     const pathsToCheckRaw = [];
     if (typeFilter === 'all' || typeFilter === 'movie') pathsToCheckRaw.push('movies');
     if (typeFilter === 'all' || typeFilter === 'tv') pathsToCheckRaw.push('tvseries');
     if (typeFilter === 'all' || typeFilter === 'anime') pathsToCheckRaw.push('anime');
     if (pathsToCheckRaw.length === 0) {
        cachedRegionIdSets[cacheKey] = new Set();
        return new Set();
     }
      const pathsToFetch = pathsToCheckRaw.map(basePath => getListPath(regionFilter, basePath)).filter(Boolean);
       if(DEBUG_LOG) console.log(`[IMDb-v1 DEBUG] getRegionIds for '${cacheKey}', paths: ${pathsToFetch.join(', ')}`);

      const promises = pathsToFetch.map(path => fetchShard(path));
      const results = await Promise.allSettled(promises);
      results.forEach((result) => {
           if (result.status === 'fulfilled' && Array.isArray(result.value)) {
                result.value.forEach(item => regionIds.add(item.id));
           } else if (result.status === 'rejected'){
                  if(DEBUG_LOG) console.warn(`[IMDb-v1 WARN] Region fetch failed for ${regionFilter}/${typeFilter}: ${result.reason}`);
           }
      });
     if(DEBUG_LOG) console.log(`[IMDb-v1 DEBUG] Region IDs for '${cacheKey}': ${regionIds.size}`);
    cachedRegionIdSets[cacheKey] = regionIds;
    return regionIds;
}

// --- 核心处理 ---
function mapToWidgetItem(item) {
    if (!item || typeof item.id === 'undefined' || item.id === null) return null;
    let mediaType = item.mt;
     if (mediaType !== 'movie' && mediaType !== 'tv') { mediaType = 'movie'; }
     mediaType = mediaType.toLowerCase(); 
     const posterUrl = buildImageUrl(IMG_BASE_POSTER, item.p);
    const widgetItem = {
        id: String(item.id), type: "tmdb", title: item.t || '未知标题',
        posterPath: posterUrl, backdropPath: buildImageUrl(IMG_BASE_BACKDROP, item.b), coverUrl: posterUrl, 
        releaseDate: item.y ? `${String(item.y)}-01-01` : '', 
        mediaType: mediaType, rating: typeof item.r === 'number' ? item.r.toFixed(1) : '0.0', description: item.o || '',
        link: null, genreTitle: "", duration: 0, durationText: "", episode: 0, childItems: []                         
    };
     return widgetItem;
}

function processData(data, params) {
     if(!Array.isArray(data) || data.length === 0) return [];
     const sortedData = [...data]; 
     const sortKeyRaw = params.sort || 'd_desc';
     const sortKey = typeof sortKeyRaw === 'string' ? sortKeyRaw.split('_desc')[0] : 'd'; 
      sortedData.sort((a,b) => {
        const valA = (a && typeof a[sortKey] === 'number') ? a[sortKey] : -Infinity;
        const valB = (b && typeof b[sortKey] === 'number') ? b[sortKey] : -Infinity;
        return valB - valA;
      });
     const page = Math.max(1, parseInt(params.page || "1", 10));
     const paginatedData = sortedData.slice((page - 1) * ITEMS_PER_PAGE, page * ITEMS_PER_PAGE);
     return paginatedData.map(mapToWidgetItem).filter(Boolean); 
}

// 路径辅助
function getCategoryPath(paramValue, basePath){
    if (!paramValue || paramValue === 'all') return null; 
     const fileName = String(paramValue).replace(':', '_');
     return `${basePath}/${fileName}.json`;
}
function getListPath(paramValue, basePath){
     const value = paramValue || 'all';
     const fileName = String(value).replace(':', '_');
     return `${basePath}/${fileName}.json`;
}

// 直接加载处理 (用于 电影/剧集/动画)
async function processRequest(params, shardPath) {
     if(DEBUG_LOG) console.log(`[IMDb-v1 DEBUG] Direct load path: ${shardPath}, Region: ${params.region}`);
    if(!shardPath) { return []; }
    try {
        const data = await fetchShard(shardPath);
        return processData(data, params);
    } catch(e) {
        console.error(`[IMDb-v1 ERROR] 处理路径 "${shardPath}" 时出错:`, e.message || e, e.stack);
        throw new Error(`加载数据失败: ${e.message || '未知错误'}`);
    }
}

// 过滤处理 (用于 热门/分类/年份)
async function applyFiltersAndProcess(baseDataPromise, params, sourceTag = 'unknown') {
      try {
         const typeFilter = params.contentType || 'all';
         const regionFilter = params.region || 'all';
          if(DEBUG_LOG) console.log(`[IMDb-v1 DEBUG] Filter START [${sourceTag}]: Type=${typeFilter}, Region=${regionFilter}`);

         const [baseData, animeIds, regionIds] = await Promise.all([
               baseDataPromise, getAnimeIds(), getRegionFilteredIds(regionFilter, typeFilter) 
         ]);

         let currentData = baseData || [];
         const initialCount = currentData.length;
         if (initialCount === 0) return [];

         // 1. Type Filter
         if (typeFilter === 'anime') {
            currentData = currentData.filter(item => animeIds.has(item.id));
        } else if (typeFilter === 'movie') {
            currentData = currentData.filter(item => item.mt === 'movie' && !animeIds.has(item.id));
        } else if (typeFilter === 'tv') {
             currentData = currentData.filter(item => item.mt === 'tv' && !animeIds.has(item.id));
        }
        const countAfterType = currentData.length;
        
         // 2. Region Filter
          if(regionIds !== null) { // null means region='all', no filtering needed
              if(regionIds.size === 0) { currentData = []; } // Region selected, but no items found for this region+type
              else { currentData = currentData.filter(item => regionIds.has(item.id)); } // Intersection
         }
         const finalCount = currentData.length;
         if(DEBUG_LOG) console.log(`[IMDb-v1 DEBUG] Filter END [${sourceTag}]: Base:${initialCount} -> Type(${typeFilter}):${countAfterType} -> Region(${regionFilter}, size:${regionIds?regionIds.size:'ALL'}):${finalCount}`);
         
          if (finalCount === 0) return [];
         return processData(currentData, params);

      } catch(e) {
          console.error(`[IMDb-v1 ERROR] Filtering error [${sourceTag}]:`, e.message || e, e.stack);
          throw new Error(`数据过滤失败: ${e.message || '未知错误'}`);
      }
}

// --- 模块入口函数 ---
async function listRecentHot(params) { 
    return applyFiltersAndProcess(fetchShard('recent_hot.json'), params, 'HOT');
}
async function listByCategory(params) {
     const category = params.category || 'all';
     const baseDataPromise = (category === 'all') ? getMasterData() : fetchShard(getCategoryPath(category, 'by_tag'));
     return applyFiltersAndProcess(baseDataPromise, params, `CAT:${category}`);
}
async function listByYear(params) { 
    const year = params.year || 'all';
     const baseDataPromise = (year === 'all') ? getMasterData() : fetchShard((year === 'all') ? null : `by_year/${year}.json`);
    return applyFiltersAndProcess(baseDataPromise, params, `YEAR:${year}`);
 }
async function listMovies(params) { return processRequest(params, getListPath(params.region, 'movies')); }
async function listTVSeries(params) {return processRequest(params, getListPath(params.region, 'tvseries')); }
async function listAnime(params) { return processRequest(params, getListPath(params.region, 'anime'));}

console.log("[IMDb-v1] Script Loaded Successfully.");
