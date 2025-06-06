// --- 核心配置 ---
const STATIC_DATA_URL = "https://raw.githubusercontent.com/opix-maker/Forward/refs/heads/main/precomputed_data.json"; // 

var WidgetMetadata = {
    id: "bangumi_charts_tmdb_v3",
    title: "Bangumi 热门榜单",
    description: "从Bangumi获取近期热门、每日放送数据，支持榜单筛选，智能匹配TMDB数据。",
    author: "Autism ",
    site: "https://github.com/opix-maker/Forward",
    version: "2.0.0-static", 
    requiredVersion: "0.0.1",
    modules: [
        {
            title: "近期热门",
            description: "按作品类型浏览近期热门内容 (固定按热度 trends 排序)",
            requiresWebView: false,
            functionName: "fetchRecentHot",
            params: [
                { name: "category", title: "分类", type: "enumeration", value: "anime", enumOptions: [ { title: "动画", value: "anime" }, { title: "三次元", value: "real" } ] },
                { name: "page", title: "页码", type: "page", value: "1" }
            ]
        },
        {
            title: "年度/季度榜单",
            description: "按年份、季度/全年及作品类型浏览排行",
            requiresWebView: false,
            functionName: "fetchAirtimeRanking",
            params: [
                { name: "category", title: "分类", type: "enumeration", value: "anime", enumOptions: [ { title: "动画", value: "anime" }, { title: "三次元", value: "real" } ] },
                { name: "year", title: "年份", type: "input", description: "例如: 2024。留空则浏览所有年份。", value: "2025" },
                { name: "month", title: "月份/季度", type: "enumeration", value: "all", description: "选择全年或特定季度对应的月份。留空则为全年。", enumOptions: [ { title: "全年", value: "all" }, { title: "冬季 (1月)", value: "1" }, { title: "春季 (4月)", value: "4" }, { title: "夏季 (7月)", value: "7" }, { title: "秋季 (10月)", value: "10" } ] },
                { name: "sort", title: "排序方式", type: "enumeration", value: "collects", enumOptions: [ { title: "排名", value: "rank" }, { title: "热度", value: "trends" }, { title: "收藏数", value: "collects" }, { title: "发售日期", value: "date" }, { title: "名称", "value": "title" } ] },
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


// --- 全局数据管理 ---
let globalPrecomputedData = null;
let dataFetchPromise = null;

async function fetchAndCacheGlobalData() {
    if (globalPrecomputedData) {
        return globalPrecomputedData;
    }
    if (dataFetchPromise) {
        return await dataFetchPromise;
    }

    dataFetchPromise = (async () => {
        try {
            console.log(`[BGM Widget v7] 开始获取预构建数据: ${STATIC_DATA_URL}`);
            const response = await Widget.http.get(STATIC_DATA_URL, {
                headers: { 'Accept': 'application/json', 'Cache-Control': 'no-cache' }
            });
            if (!response || !response.data) {
                throw new Error("预构建数据响应为空或无效");
            }
            globalPrecomputedData = response.data;
            console.log(`[BGM Widget v7] 预构建数据获取成功，构建于: ${new Date(globalPrecomputedData.buildTimestamp).toLocaleString()}`);
            return globalPrecomputedData;
        } catch (error) {
            console.error(`[BGM Widget v7] 获取预构建数据失败:`, error.message);
            dataFetchPromise = null; // 允许重试
            throw error; // 向上抛出错误
        }
    })();

    return await dataFetchPromise;
}


// --- 模块实现 (纯数据过滤) ---

async function fetchRecentHot(params = {}) {
    const data = await fetchAndCacheGlobalData();
    if (!data || !data.recentHot) return [];

    const category = params.category || "anime";
    const page = parseInt(params.page || "1", 10);

    // 假设 precomputed_data.json 结构为: { recentHot: { anime: [ [page1_items], [page2_items] ], real: [...] } }
    const pages = data.recentHot[category] || [];
    return pages[page - 1] || [];
}

async function fetchAirtimeRanking(params = {}) {
    const data = await fetchAndCacheGlobalData();
    if (!data || !data.airtimeRanking) return [];

    const category = params.category || "anime";
    const year = params.year || "2025";
    const month = params.month || "all";
    const sort = params.sort || "collects";
    const page = parseInt(params.page || "1", 10);

    // 假设 precomputed_data.json 结构为: { airtimeRanking: { anime: { "2025": { "all": { "collects": [ [page1_items], ... ] } } } } }
    try {
        const pages = data.airtimeRanking[category][year][month][sort] || [];
        return pages[page - 1] || [];
    } catch (e) {
        console.warn(`[BGM Widget v7] 在预构建数据中未找到路径: ${category}.${year}.${month}.${sort}`);
        return [];
    }
}

async function fetchDailyCalendarApi(params = {}) {
    const data = await fetchAndCacheGlobalData();
    if (!data || !data.dailyCalendar) return [];

    const filterType = params.filterType || "today";
    const specificWeekday = params.specificWeekday || "1";
    const sortOrder = params.dailySortOrder || "popularity_rat_bgm";
    const regionFilter = params.dailyRegionFilter || "all";

    // 假设 precomputed_data.json 结构为: { dailyCalendar: { all_week: [item1, item2], ... } }
    // 筛选和排序逻辑在客户端执行，以保持UI的灵活性
    
    const JS_DAY_TO_BGM_API_ID = { 0: 7, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6 };
    const REGION_FILTER_US_EU_COUNTRIES = ["US", "GB", "FR", "DE", "CA", "AU", "ES", "IT"];
    
    let items = data.dailyCalendar.all_week || [];

    // 1. 按星期筛选
    let filteredByDay = [];
    if (filterType === "all_week") {
        filteredByDay = items;
    } else {
        const today = new Date();
        const currentJsDay = today.getDay(); // 0 for Sunday, 1 for Monday...
        
        const targetBgmIds = new Set();
        switch (filterType) {
            case "today":
                targetBgmIds.add(JS_DAY_TO_BGM_API_ID[currentJsDay]);
                break;
            case "specific_day":
                targetBgmIds.add(parseInt(specificWeekday, 10));
                break;
            case "mon_thu":
                [1, 2, 3, 4].forEach(id => targetBgmIds.add(id));
                break;
            case "fri_sun":
                [5, 6, 7].forEach(id => targetBgmIds.add(id));
                break;
        }
        filteredByDay = items.filter(item => targetBgmIds.has(item.bgm_weekday_id));
    }

    // 2. 按地区筛选
    let filteredByRegion = filteredByDay;
    if (regionFilter !== "all") {
        filteredByRegion = filteredByDay.filter(item => {
            if (item.type !== "tmdb" || !item.tmdb_id) {
                return regionFilter === "OTHER"; // BGM原生条目归为其他
            }
            const countries = item.tmdb_origin_countries || [];
            if (countries.length === 0) return regionFilter === "OTHER";
            if (regionFilter === "JP") return countries.includes("JP");
            if (regionFilter === "CN") return countries.includes("CN");
            if (regionFilter === "US_EU") return countries.some(c => REGION_FILTER_US_EU_COUNTRIES.includes(c));
            if (regionFilter === "OTHER") {
                const isJPCNUSEU = countries.includes("JP") || countries.includes("CN") || countries.some(c => REGION_FILTER_US_EU_COUNTRIES.includes(c));
                return !isJPCNUSEU;
            }
            return false;
        });
    }

    // 3. 排序
    let sortedResults = filteredByRegion;
    if (sortOrder !== "default") {
        sortedResults.sort((a, b) => {
            if (sortOrder === "popularity_rat_bgm") return (b.bgm_rating_total || 0) - (a.bgm_rating_total || 0);
            if (sortOrder === "score_bgm_desc") return (b.bgm_score || 0) - (a.bgm_score || 0);
            if (sortOrder === "airdate_desc") {
                const dateA = a.releaseDate || a.bgm_air_date || 0;
                const dateB = b.releaseDate || b.bgm_air_date || 0;
                return new Date(dateB).getTime() - new Date(dateA).getTime();
            }
            return 0;
        });
    }
    
    return sortedResults;
}
