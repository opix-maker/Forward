const GITHUB_OWNER = "opix-maker";
const GITHUB_REPO = "Forward";
const GITHUB_BRANCH = "main";
const BASE_DATA_URL = `https://raw.githubusercontent.com/${GITHUB_OWNER}/${GITHUB_REPO}/${GITHUB_BRANCH}/imdb-data-platform/dist`;
const IMG_BASE_POSTER = 'https://image.tmdb.org/t/p/w500';
const IMG_BASE_BACKDROP = 'https://image.tmdb.org/t/p/w780'; 
const ITEMS_PER_PAGE = 30;
const CURRENT_YEAR = new Date().getFullYear();

console.log(`[IMDb-v1] 脚本初始化.`);

 function buildImageUrl(baseUrl, path) {
     if (!path || typeof path !== 'string') { return null; }
     if (path.startsWith('http://') || path.startsWith('https://')) { return path; }
      const cleanPath = path.startsWith('/') ? path : '/' + path;
      return baseUrl + cleanPath;
 }

function sortOptionsWithAllFirst(options, allOptionValue = "all") {
     const sortedOptions = [...options];
     const allItemIndex = sortedOptions.findIndex(opt => opt.value === allOptionValue);
     let allItem = null;
      if (allItemIndex > -1) {
        allItem = sortedOptions.splice(allItemIndex, 1)[0];
      }
     sortedOptions.sort((a,b) => a.title.localeCompare(b.title, 'zh-Hans-CN'));
      if(allItem) {
         sortedOptions.unshift(allItem);
      } else if (allOptionValue) {
          const allTitle = allOptionValue === 'all' ? "全部" : "全部";
           sortedOptions.unshift({ title: allTitle, value: allOptionValue});
      }
    return sortedOptions;
}

const pageParam = { name: "page", title: "页码", type: "page", value: "1" };
const sortOptions = [
    { title: "🔥综合热度", value: "hs_desc" }, { title: "👍评分", value: "r_desc" }, { title: "默认排序", value: "d_desc" }
];
const sortParam = (defaultValue = "hs_desc") => ({ name: "sort", title: "排序方式", type: "enumeration", value: defaultValue, enumOptions: sortOptions });

const yearOptions = [{ title: "全部", value: "all" }];
for(let y = CURRENT_YEAR; y >= 1990 ; y--) {
     yearOptions.push({title: `${y} 年`, value: String(y)});
 }
const yearEnumParam = { name: "year", title: "年份", type: "enumeration", value: "all", description:"选择特定年份", enumOptions: yearOptions };

const regionOptionsRaw = [
 { title: "华语", value: "region:chinese" }, { title: "中国大陆", value: "country:cn" }, { title: "香港", value: "country:hk" }, { title: "台湾", value: "country:tw" },
 { title: "欧美", value: "region:us-eu" }, { title: "美国", value: "country:us" }, { title: "英国", value: "country:gb" }, { title: "日韩", value: "region:east-asia"},{ title: "日本", value: "country:jp" }, { title: "韩国", value: "country:kr" },
 { title: "法国", value: "country:fr" }, { title: "德国", value: "country:de" }, { title: "加拿大", value: "country:ca" }, {title: "澳大利亚", value: "country:au"}
];
const regionParamSelect =  { name: "region", title: "选择地区/语言", type: "enumeration", value: "all", enumOptions: sortOptionsWithAllFirst(regionOptionsRaw, "all")};

const genreMap = [
 { title: "爱情", value: "genre:爱情" },{ title: "冒险", value: "genre:冒险" },{ title: "悬疑", value: "genre:悬疑" }, { title: "惊悚", value: "genre:惊悚" },{ title: "恐怖", value: "genre:恐怖" },{ title: "科幻", value: "genre:科幻" },
 { title: "奇幻", value: "genre:奇幻" },{ title: "动作", value: "genre:动作" },{ title: "喜剧", value: "genre:喜剧" }, { title: "剧情", value: "genre:剧情" }, { title: "历史", value: "genre:历史" },{ title: "战争", value: "genre:战争" },{ title: "犯罪", value: "genre:犯罪" },
];
const themeOptionsRaw = [
 { title: "赛博朋克", value: "theme:cyberpunk" }, { title: "太空歌剧", value: "theme:space-opera" }, { title: "时间旅行", value: "theme:time-travel" }, { title: "末世废土", value: "theme:post-apocalyptic" }, { title: "机甲", value: "theme:mecha" },{ title: "丧尸", value: "theme:zombie" }, { title: "怪物", value: "theme:monster" }, { title: "灵异", value: "theme:ghost" }, { title: "魔法", value: "theme:magic" },{ title: "黑帮", value: "theme:gangster" }, { title: "黑色电影", value: "theme:film-noir" }, { title: "连环杀手", value: "theme:serial-killer" },{ title: "仙侠", value: "theme:xianxia" }, { title: "怪兽(Kaiju)", value: "theme:kaiju" }, { title: "异世界", value: "theme:isekai" },
  { title: "侦探推理", value: "theme:whodunit" },{ title: "谍战", value: "theme:spy" },{ title: "律政", value: "theme:courtroom" }, { title: "校园/日常", value: "theme:slice-of-life" }, { title: "武侠", value: "theme:wuxia" }, { title: "超级英雄", value: "theme:superhero" }
];
const allCategoryOptions = [...genreMap, ...themeOptionsRaw];
const categoryParam = { name: "category", title: "选择分类/主题", type: "enumeration", value: "all", enumOptions: sortOptionsWithAllFirst(allCategoryOptions) };

const hotTypeParam = { 
    name: "hotType", title: "内容分类", type: "enumeration", value: "all", 
    enumOptions: [
       {title:"🔥全部", value:"all"}, {title:"🎬电影", value:"movie"}, 
       {title:"📺剧集", value:"tv"}, {title:"✨动画",value:"anime"}
    ]
 };

var WidgetMetadata = {
    id: "imdb_discovery_v1",
    title: "IMDb 分类资源 (v1)",
    description: "聚合 IMDb 热门影视资源",
    author: "Autism",
    site: "https://github.com/opix-maker/Forward",
    version: "1.0.2",
    requiredVersion: "0.0.1",
    detailCacheDuration: 3600,
    cacheDuration: 18000,
    modules: [
        { title: "🆕 近期热门",   functionName: "listRecentHot",   params: [hotTypeParam, regionParamSelect, sortParam("hs_desc"), pageParam], cacheDuration: 1800, requiresWebView: false },
        { title: "🎭 分类/主题", functionName: "listByCategory",  params: [categoryParam, sortParam(), pageParam], cacheDuration: 1800, requiresWebView: false },
        { title: "📅 按年份浏览", functionName: "listByYear",      params: [yearEnumParam, sortParam("d_desc"), pageParam], cacheDuration: 1800, requiresWebView: false },
        { title: "🎬 电影",       functionName: "listMovies",      params: [regionParamSelect, sortParam(), pageParam], cacheDuration: 1800, requiresWebView: false },
        { title: "📺 剧集",       functionName: "listTVSeries",    params: [regionParamSelect, sortParam(), pageParam], cacheDuration: 1800, requiresWebView: false },
        { title: "✨ 动画",       functionName: "listAnime",       params: [regionParamSelect, sortParam(), pageParam], cacheDuration: 1800, requiresWebView: false },
   ]
};

let cachedData = {};
let animeIdCache = null; 

 function getCacheBuster() {
    return Math.floor(Date.now() / (1000 * 60 * 20));
 }

async function fetchShard(shardPath) {
    if (!shardPath || typeof shardPath !== 'string' || !shardPath.endsWith('.json')) {
       console.warn(`[IMDb-v1 WARN] 无效的分片路径: ${shardPath}`);
       return [];
     }
    const url = `${BASE_DATA_URL}/${shardPath}?cache_buster=${getCacheBuster()}`;
     if (cachedData[url]) { return cachedData[url]; }
    let response;
    try {
        response = await Widget.http.get(url, { timeout: 30000, headers: {'User-Agent': 'ForwardWidget/IMDb-v1'} }); 
    } catch (e) { throw new Error(`网络请求失败: ${e.message || '未知网络错误'}`);}
    if (!response || typeof response.statusCode !== 'number' || response.statusCode !== 200 || !response.data ) {
       console.error(`[IMDb-v1 ERROR] 获取数据响应异常. Status: ${response ? response.statusCode : 'N/A'}, URL: ${url}`);
        throw new Error(`获取数据失败 (Status: ${response ? response.statusCode : 'N/A'})`);
    }
      const data = Array.isArray(response.data) ? response.data : [];
    cachedData[url] = data;
    return data;
}

async function getAnimeIds() {
    if (animeIdCache) {
        return animeIdCache;
    }
    try {
       const allAnimeData = await fetchShard('anime/all.json');
       animeIdCache = new Set(allAnimeData.map(item => item.id));
       return animeIdCache;
    } catch(e) {
        console.error("[IMDb-v1 ERROR] Failed to fetch anime IDs:", e);
        return new Set(); 
    }
}

function mapToWidgetItem(item) {
    if (!item || typeof item.id === 'undefined' || item.id === null) return null;
    let mediaType = item.mt;
     if (mediaType !== 'movie' && mediaType !== 'tv') { mediaType = 'movie'; }
     mediaType = mediaType.toLowerCase(); 
     const tmdbId = item.id; 
    const ratingValue = typeof item.r === 'number' ? item.r.toFixed(1) : '0.0';
    const posterUrl = buildImageUrl(IMG_BASE_POSTER, item.p);
    const backdropUrl = buildImageUrl(IMG_BASE_BACKDROP, item.b);
    const widgetItem = {
        id: String(tmdbId), type: "tmdb", title: item.t || '未知标题',
        posterPath: posterUrl, backdropPath: backdropUrl, coverUrl: posterUrl, 
        releaseDate: item.y ? `${String(item.y)}-01-01` : '', 
        mediaType: mediaType, rating: ratingValue, description: item.o || '',
        link: null,
         genreTitle: "", duration: 0, durationText: "", 
         episode: 0, childItems: []                         
    };
     return widgetItem;
}

function processData(data, params) {
     if(!Array.isArray(data)) data = [];
     const sortedData = [...data]; 
     const sortKeyRaw = params.sort || 'd_desc';
     const sortKey = typeof sortKeyRaw === 'string' ? sortKeyRaw.split('_desc')[0] : 'd'; 
      sortedData.sort((a,b) => {
        const valA = (a && typeof a[sortKey] === 'number') ? a[sortKey] : -Infinity;
        const valB = (b && typeof b[sortKey] === 'number') ? b[sortKey] : -Infinity;
        return valB - valA;
      });
     const page = Math.max(1, parseInt(params.page || "1", 10));
     const startIndex = (page - 1) * ITEMS_PER_PAGE;
     const endIndex = page * ITEMS_PER_PAGE;
     const paginatedData = sortedData.slice(startIndex, endIndex);
     return paginatedData.map(mapToWidgetItem).filter(Boolean); 
}

async function processRequest(params, shardPath) {
    if(!shardPath) { return []; }
    try {
        const data = await fetchShard(shardPath);
        return processData(data, params);
    } catch(e) {
        console.error(`[IMDb-v1 ERROR] 处理路径 "${shardPath}" 时出错:`, e.message || e, e.stack);
        throw new Error(`加载数据失败: ${e.message || '未知错误'}`);
    }
}

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

async function listRecentHot(params) { 
     try {
        const allHotData = await fetchShard('recent_hot.json');
        const typeFilter = params.hotType || 'all';
        const regionFilter = params.region || 'all';
        let currentData = allHotData;
        const animeIds = await getAnimeIds();
        
         // 1. Filter by Type
        if (typeFilter === 'anime') {
            currentData = currentData.filter(item => animeIds.has(item.id));
        } else if (typeFilter === 'movie') {
            currentData = currentData.filter(item => item.mt === 'movie' && !animeIds.has(item.id));
        } else if (typeFilter === 'tv') {
             currentData = currentData.filter(item => item.mt === 'tv' && !animeIds.has(item.id));
        }
        
         // 2. Filter by Region (based on IDs from region files)
         if (regionFilter !== 'all') {
             const regionIds = new Set();
             const pathsToCheck = [];
             if (typeFilter === 'all' || typeFilter === 'movie') pathsToCheck.push('movies');
             if (typeFilter === 'all' || typeFilter === 'tv') pathsToCheck.push('tvseries');
             if (typeFilter === 'all' || typeFilter === 'anime') pathsToCheck.push('anime');

              // Load IDs from all relevant region files
              for (const basePath of pathsToCheck) {
                   const path = getListPath(regionFilter, basePath);
                   if (path) {
                        try {
                           const regionData = await fetchShard(path);
                            regionData.forEach(item => regionIds.add(item.id));
                        } catch(e) { console.warn(`[IMDb-v1 WARN] 无法加载区域数据: ${path}`);}
                   }
              }
              // Keep only hot items whose ID is present in the region set
               currentData = currentData.filter(item => regionIds.has(item.id));
         }

        return processData(currentData, params);
     } catch(e) {
        console.error(`[IMDb-v1 ERROR] 处理 listRecentHot 时出错:`, e.message || e, e.stack);
        throw new Error(`加载热门数据失败: ${e.message || '未知错误'}`);
     }
}

async function listByCategory(params) {
    return processRequest(params, getCategoryPath(params.category, 'by_tag'));
}

async function listByYear(params) { 
    const year = params.year || 'all';
    const shardPath = (year === 'all') ? null : `by_year/${year}.json`;
    return processRequest(params, shardPath);
 }
async function listMovies(params) { return processRequest(params, getListPath(params.region, 'movies')); }
async function listTVSeries(params) {return processRequest(params, getListPath(params.region, 'tvseries')); }
async function listAnime(params) { return processRequest(params, getListPath(params.region, 'anime'));}

console.log("[IMDb-v1] Script Loaded Successfully.");
