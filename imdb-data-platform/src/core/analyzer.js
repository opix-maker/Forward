// --- 关键词映射 ---
const KEYWORD_TO_THEME_MAP = {
    // 科幻
    'cyberpunk': 'theme:cyberpunk', 'dystopia': 'theme:cyberpunk', 'virtual reality': 'theme:cyberpunk', 'artificial intelligence': 'theme:cyberpunk', 'neo-noir': 'theme:cyberpunk',
    'space opera': 'theme:space-opera', 'alien': 'theme:space-opera', 'galaxy': 'theme:space-opera', 'spaceship': 'theme:space-opera',
    'time travel': 'theme:time-travel', 'time loop': 'theme:time-travel', 'paradox': 'theme:time-travel',
    'post-apocalyptic': 'theme:post-apocalyptic', 'dystopian': 'theme:post-apocalyptic',
    'superhero': 'theme:superhero', 'marvel comics': 'theme:superhero', 'dc comics': 'theme:superhero', 'comic book': 'theme:superhero',
    'mecha': 'theme:mecha', 'giant robot': 'theme:mecha',
    // 奇幻/恐怖
    'zombie': 'theme:zombie', 'undead': 'theme:zombie',
    'vampire': 'theme:vampire', 'werewolf': 'theme:werewolf', 'monster': 'theme:monster', 'kaiju': 'theme:kaiju',
    'ghost': 'theme:ghost', 'haunting': 'theme:ghost', 'supernatural horror': 'theme:ghost',
    'slasher': 'theme:slasher', 'body horror': 'theme:body-horror', 'folk horror': 'theme:folk-horror',
    // 奇幻
    'magic': 'theme:magic', 'sword and sorcery': 'theme:magic',
    // 犯罪/悬疑
    'gangster': 'theme:gangster', 'mafia': 'theme:gangster', 'mobster': 'theme:gangster',
    'heist': 'theme:heist', 'film-noir': 'theme:film-noir', 'hardboiled': 'theme:film-noir',
    'conspiracy': 'theme:conspiracy', 'spy': 'theme:spy', 'espionage': 'theme:spy', 'assassin': 'theme:assassin',
    'serial killer': 'theme:serial-killer', 'whodunit': 'theme:whodunit', 'courtroom drama': 'theme:courtroom',
    // 亚洲文化
    'wuxia': 'theme:wuxia', 'martial arts': 'theme:wuxia', 'kung fu': 'theme:wuxia',
    'xianxia': 'theme:xianxia', 'samurai': 'theme:samurai', 'ninja': 'theme:ninja', 'yakuza': 'theme:yakuza',
    'tokusatsu': 'theme:tokusatsu',
    'isekai': 'theme:isekai', 'slice of life': 'theme:slice-of-life', 'high school': 'theme:slice-of-life',
    // 其他
    'found footage': 'theme:found-footage',
};

// --- 类型门控 ---
const GATED_THEMES = {
    'theme:ghost': ['恐怖', '惊悚', '悬疑'],
    'theme:zombie': ['恐怖'],
    'theme:vampire': ['恐怖', '奇幻'],
    'theme:werewolf': ['恐怖', '奇幻'],
    'theme:monster': ['恐怖', '科幻', '奇幻'],
    'theme:slasher': ['恐怖', '惊悚'],
    'theme:body-horror': ['恐怖'],
    'theme:kaiju': ['科幻', '动作', '恐怖'],
    'theme:serial-killer': ['犯罪', '惊悚', '恐怖', '悬疑'],
};

// --- 预编译正则表达式 ---
const COMPILED_REGEX_MAP = {};
for (const [keyword, theme] of Object.entries(KEYWORD_TO_THEME_MAP)) {
    COMPILED_REGEX_MAP[keyword] = {
        regex: new RegExp(`\\b${keyword.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&')}\\b`, 'i'),
        theme: theme
    };
}

// --- 语言到国家映射  ---
const LANGUAGE_TO_COUNTRY = {
    'zh': 'cn', 'ja': 'jp', 'ko': 'kr', 'en': 'us', 'fr': 'fr', 'de': 'de', 'es': 'es', 'it': 'it',
};

// --- IMDb 英文类型到中文的映射 ---
const IMDB_GENRE_TO_CHINESE = {
    'Action': '动作',
    'Adventure': '冒险',
    'Animation': '动画',
    'Biography': '传记',
    'Comedy': '喜剧',
    'Crime': '犯罪',
    'Documentary': '纪录片',
    'Drama': '剧情',
    'Family': '家庭',
    'Fantasy': '奇幻',
    'Film-Noir': '黑色电影', // 注意：IMDb 视其为类型，我们也将其视为类型
    'History': '历史',
    'Horror': '恐怖',
    'Music': '音乐',
    'Musical': '歌舞',
    'Mystery': '悬疑',
    'Romance': '爱情',
    'Sci-Fi': '科幻',
    'Sport': '运动',
    'Thriller': '惊悚',
    'War': '战争',
    'Western': '西部',
    'Short': '短片',
    'News': '新闻',
    'Reality-TV': '真人秀',
    'Talk-Show': '脱口秀',
    'Game-Show': '游戏节目',
};



export function analyzeAndTagItem(item, imdbAkasInfo = { regions: new Set(), languages: new Set() }, imdbGenresString = null) {
    if (!item) return null;

    const tags = new Set();
    // --- 确定日期和年份 ---
    const release_date = item.release_date || item.first_air_date;
    const year = release_date ? new Date(release_date).getFullYear() : null;


    // 1. 基础类型标签 (type:movie, type:tv)
    const mediaType = item.media_type || (item.seasons ? 'tv' : 'movie');
    tags.add(`type:${mediaType}`);

    // 2. 类型标签 (重大改进：合并 TMDB 和 IMDb 类型)
    const genreNames = new Set();
    
    // 2a. 添加 TMDB 类型 (API 请求 zh-CN，所以已经是中文)
    (item.genres || []).forEach(g => { if (g.name) genreNames.add(g.name); });

    // 2b. 添加并翻译 IMDb 类型
    if (imdbGenresString && imdbGenresString !== '\\N') {
        const imdbGenres = imdbGenresString.split(',');
        imdbGenres.forEach(genreEn => {
            // 根据映射表翻译成中文
            const genreCn = IMDB_GENRE_TO_CHINESE[genreEn.trim()];
            if (genreCn) {
                genreNames.add(genreCn); // 添加到集合中自动去重
            }
        });
    }

    // 应用类型标签
    genreNames.forEach(name => tags.add(`genre:${name}`));
    
    // 判断是否为动画 (基于合并后的类型)
    const isAnimation = genreNames.has('动画');
    if (isAnimation) {
        tags.add('type:animation');
    }

    // 3. 年代标签 (decade:1990s)
    if (year) {
        tags.add(`decade:${Math.floor(year / 10) * 10}s`);
    }

    // 4. 制作国家/地区/语言标签
    const countries = new Set();
    (item.origin_country || []).forEach(c => countries.add(c.toLowerCase()));
    (item.production_countries || []).forEach(pc => countries.add(pc.iso_3166_1.toLowerCase()));
    imdbAkasInfo.regions.forEach(r => countries.add(r));
    imdbAkasInfo.languages.forEach(l => {
        const country = LANGUAGE_TO_COUNTRY[l];
        if (country) countries.add(country);
    });
    countries.forEach(c => tags.add(`country:${c}`));

    // 5. 聚合地区标签 
    if (countries.has('us') || countries.has('gb') || countries.has('fr') || countries.has('de') || countries.has('ca') || countries.has('au') || countries.has('it') || countries.has('es')) {
        tags.add('region:us-eu');
    }
    if (countries.has('jp') || countries.has('kr')) {
        tags.add('region:east-asia');
    }
    if (imdbAkasInfo.languages.has('zh') || countries.has('cn') || countries.has('hk') || countries.has('tw')) {
        tags.add('region:chinese');
    }


    // 6. 关键词/主题/风格/情绪标签 
    const tmdbKeywords = (item.keywords?.keywords || item.keywords?.results || []).map(k => k.name.toLowerCase());
    const foundThemes = new Set();

    tmdbKeywords.forEach(tmdbKeyword => {
        for (const { regex, theme } of Object.values(COMPILED_REGEX_MAP)) {
            if (regex.test(tmdbKeyword)) {
                foundThemes.add(theme);
            }
        }
    });

    // 应用类型门控 
    foundThemes.forEach(theme => {
        const requiredGenres = GATED_THEMES[theme];
        if (requiredGenres) {
            const genreGatePassed = requiredGenres.some(requiredGenre => genreNames.has(requiredGenre));
            if (genreGatePassed) {
                tags.add(theme);
            }
        } else {
            tags.add(theme);
        }
    });

    // 7. 聚合分类标签 
    if (tags.has('type:tv')) {
        if (tags.has('region:us-eu')) tags.add('category:us-eu_tv');
        if (tags.has('region:east-asia')) tags.add('category:east-asia_tv');
        if (tags.has('region:chinese')) tags.add('category:chinese_tv');
    }
    if (tags.has('type:animation')) {
        if (tags.has('country:jp')) tags.add('category:jp_anime');
        if (tags.has('country:cn')) tags.add('category:cn_anime');
    }

    // --- 提取中文信息 ---
    const chineseTranslation = item.translations?.translations?.find(t => t.iso_639_1 === 'zh' || t.iso_639_1 === 'zh-CN');
    const title_zh = chineseTranslation?.data?.title || chineseTranslation?.data?.name || item.title || item.name;
    const overview_zh = chineseTranslation?.data?.overview || item.overview;

    // --- 确定统一的 mediaType ---
    let finalMediaType = 'movie'; 
    if (tags.has('type:tv')) {
        finalMediaType = 'tv';
    }
    if (tags.has('type:animation')) {
        finalMediaType = 'anime'; 
    }

    // 返回 Data Lake 中的标准数据结构
    return {
        id: item.id,
        imdb_id: item.external_ids?.imdb_id,
        title: title_zh,
        overview: overview_zh,
        poster_path: item.poster_path,
        backdrop_path: item.backdrop_path,
        release_date: release_date,
        release_year: year,
        vote_average: item.vote_average,
        vote_count: item.vote_count,
        popularity: item.popularity,
        mediaType: finalMediaType,
        semantic_tags: Array.from(tags),
    };
}
