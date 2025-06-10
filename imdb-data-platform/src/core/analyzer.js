const KEYWORD_TO_THEME_MAP = {
    // 科幻
    'cyberpunk': 'theme:cyberpunk', 'dystopia': 'theme:cyberpunk', 'virtual reality': 'theme:cyberpunk', 'artificial intelligence': 'theme:cyberpunk', 'neo-noir': 'theme:cyberpunk',
    'space opera': 'theme:space-opera', 'alien': 'theme:space-opera', 'galaxy': 'theme:space-opera', 'spaceship': 'theme:space-opera',
    'time travel': 'theme:time-travel', 'time loop': 'theme:time-travel', 'paradox': 'theme:time-travel',
    'post-apocalyptic': 'theme:post-apocalyptic', 'dystopian': 'theme:post-apocalyptic',
    'superhero': 'theme:superhero', 'marvel comics': 'theme:superhero', 'dc comics': 'theme:superhero', 'comic book': 'theme:superhero', 'based on comic': 'theme:superhero',
    'mecha': 'theme:mecha', 'giant robot': 'theme:mecha',
    // 奇幻/恐怖
    'zombie': 'theme:zombie', 'undead': 'theme:zombie',
    'vampire': 'theme:vampire', 'werewolf': 'theme:werewolf', 'monster': 'theme:monster',
    'ghost': 'theme:ghost', 'haunting': 'theme:ghost', 'supernatural horror': 'theme:ghost',
    'magic': 'theme:magic', 'sword and sorcery': 'theme:magic', 'witch': 'theme:magic', 'wizard': 'theme:magic',
    // 犯罪/悬疑
    'gangster': 'theme:gangster', 'mafia': 'theme:gangster', 'mobster': 'theme:gangster',
    'heist': 'theme:heist', 'film-noir': 'theme:film-noir', 'hardboiled': 'theme:film-noir',
    'conspiracy': 'theme:conspiracy', 'spy': 'theme:spy', 'espionage': 'theme:spy', 'assassin': 'theme:assassin',
    'serial killer': 'theme:serial-killer',
    'whodunit': 'theme:whodunit', 'detective': 'theme:whodunit',
     'courtroom drama': 'theme:courtroom', 'courtroom': 'theme:courtroom', 'lawyer': 'theme:courtroom', 'judge': 'theme:courtroom',
    // 亚洲文化
    'wuxia': 'theme:wuxia', 'martial arts': 'theme:wuxia', 'kung fu': 'theme:wuxia',
    'xianxia': 'theme:xianxia', 'samurai': 'theme:samurai', 'ninja': 'theme:ninja', 'yakuza': 'theme:yakuza',
    'kaiju': 'theme:kaiju', 'tokusatsu': 'theme:tokusatsu',
    'isekai': 'theme:isekai',
    'slice of life': 'theme:slice-of-life', 'high school': 'theme:slice-of-life',
    'oscar (best picture)': 'award:oscar-winner', 'golden globe (best motion picture)': 'award:golden-globe-winner', 'palme d\'or': 'award:cannes-winner',
};

// --- 映射 TMDB Genre 名称到中文 ---
 const TMDB_GENRE_TO_ZH_MAP = {
    'Action': '动作', 'Adventure': '冒险', 'Animation': '动画', 'Comedy': '喜剧',
    'Crime': '犯罪', 'Documentary': '纪录', 'Drama': '剧情', 'Family': '家庭',
    'Fantasy': '奇幻', 'History': '历史', 'Horror': '恐怖', 'Music': '音乐',
    'Mystery': '悬疑', 'Romance': '爱情', 'Science Fiction': '科幻', 'TV Movie': '电视电影',
    'Thriller': '惊悚', 'War': '战争', 'Western': '西部',
     'Action & Adventure': '动作', // for TV
     'Sci-Fi & Fantasy': '科幻', // for TV
 };

// -- 区域/国家/语言定义 ---
const CHINESE_CODES = new Set(['cn', 'hk', 'tw', 'mo', 'sg']); // 增加澳门、新加坡
const CHINESE_LANGS = new Set(['zh', 'cmn', 'yue', 'nan']); // 中文, 普通话, 粤语, 闽南语

const EAST_ASIA_CODES = new Set(['jp', 'kr', 'kp']);
const EAST_ASIA_LANGS = new Set(['ja', 'ko']);

const US_EU_CODES = new Set(['us', 'ca', 'gb', 'fr', 'de', 'it', 'es', 'au', 'nz', 'ie', 'be', 'nl', 'ch', 'at', 'se', 'dk', 'no', 'fi', 'is']); // 增加更多欧美国家
const US_EU_LANGS = new Set(['en', 'fr', 'de', 'it', 'es']);


// 辅助函数 检查集合交集
const hasIntersection = (setA, setB) => {
    for (const item of setA) {
        if (setB.has(item)) return true;
    }
    return false;
};

// 核心分析函数
export function analyzeAndTagItem(item) {
    if (!item) return null;

    const tags = new Set();
    const year = item.release_date ? new Date(item.release_date).getFullYear() : (item.first_air_date ? new Date(item.first_air_date).getFullYear() : null);

    // 1. 基础类型标签 (type:movie, type:tv)
    const mediaType = item.media_type || (item.seasons ? 'tv' : 'movie');
    tags.add(`type:${mediaType}`);

    // 2. 类型标签 (genre:科幻, genre:动画) - 使用映射
    const zhGenreNames = new Set();
    (item.genres || []).forEach(g => {
         if (g.name && TMDB_GENRE_TO_ZH_MAP[g.name]) {
            const zhName = TMDB_GENRE_TO_ZH_MAP[g.name];
            zhGenreNames.add(zhName);
            tags.add(`genre:${zhName}`); // 准确性优化: 确保 tag 与主脚本 GENRES_AND_THEMES 一致
         }
    });
     // 使用中文名判断动画
    if (zhGenreNames.has('动画')) {
        tags.add('type:animation');
    }

    // 3. 年代标签 (decade:1990s)
    if (year) {
        tags.add(`decade:${Math.floor(year / 10) * 10}s`);
    }

    const countries = new Set();
    (item.origin_country || []).forEach(c => countries.add(c.toLowerCase()));
    (item.production_countries || []).forEach(pc => countries.add(pc.iso_3166_1.toLowerCase()));
     countries.forEach(c => tags.add(`country:${c}`));

     const languages = new Set();
     (item.spoken_languages || []).forEach(lang => languages.add(lang.iso_639_1.toLowerCase()));
     //languages.forEach(l => tags.add(`lang:${l}`)); // 可选：添加语言标签

    // 5. 聚合地区标签 (region:us-eu, region:east-asia, region:chinese) - 结合国家和语言
    if (hasIntersection(US_EU_CODES, countries) || hasIntersection(US_EU_LANGS, languages)) tags.add('region:us-eu');
    if (hasIntersection(EAST_ASIA_CODES, countries) || hasIntersection(EAST_ASIA_LANGS, languages)) tags.add('region:east-asia');
    if (hasIntersection(CHINESE_CODES, countries) || hasIntersection(CHINESE_LANGS, languages)) tags.add('region:chinese');
    // --- 优化结束 ---


    // 6. 关键词/主题/风格/情绪标签
    const keywords = new Set((item.keywords?.keywords || item.keywords?.results || []).map(k => k.name.toLowerCase()));
     // Set 查找 + 完善映射 (此处保持includes以匹配原意图，但确保MAP覆盖)
     keywords.forEach(keyword => {
         for (const [mapKey, theme] of Object.entries(KEYWORD_TO_THEME_MAP)) {
             if (keyword.includes(mapKey)) {
                tags.add(theme);
             }
         }
     });

    // 7. 移除冗余的 category 标签 (准确性/效率优化 A3)
    // --- 已移除 ---

    const chineseTranslation = item.translations?.translations?.find(t => t.iso_639_1 === 'zh' && (t.iso_3166_1 === 'CN' || t.iso_3166_1 === ''));
     const defaultTitle = item.title || item.name;
     const defaultOverview = item.overview;
    const title_zh = chineseTranslation?.data?.title || chineseTranslation?.data?.name || item.original_language !== 'zh' ? defaultTitle : (item.original_title || item.original_name || defaultTitle);
    const overview_zh = chineseTranslation?.data?.overview || defaultOverview;

    return {
        id: item.id,
        imdb_id: item.external_ids?.imdb_id,
        title: title_zh || 'Unknown Title',
        overview: overview_zh,
        poster_path: item.poster_path,
        backdrop_path: item.backdrop_path,
        release_date: item.release_date || item.first_air_date,
        release_year: year,
        vote_average: item.vote_average,
        vote_count: item.vote_count,
        popularity: item.popularity,
        belongs_to_collection: item.belongs_to_collection,
        semantic_tags: Array.from(tags), // 确保输出是 Array
    };
}
