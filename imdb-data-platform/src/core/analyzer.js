const KEYWORD_TO_THEME_MAP = {
    'cyberpunk': 'theme:cyberpunk', 'dystopia': 'theme:cyberpunk', 'virtual reality': 'theme:cyberpunk', 'artificial intelligence': 'theme:cyberpunk', 'neo-noir': 'theme:cyberpunk',
    'space opera': 'theme:space-opera', 'alien': 'theme:space-opera', 'galaxy': 'theme:space-opera', 'spaceship': 'theme:space-opera',
    'time travel': 'theme:time-travel', 'time loop': 'theme:time-travel', 'paradox': 'theme:time-travel',
    'post-apocalyptic': 'theme:post-apocalyptic', 'dystopian': 'theme:post-apocalyptic',
    'superhero': 'theme:superhero', 'marvel comics': 'theme:superhero', 'dc comics': 'theme:superhero', 'comic book': 'theme:superhero',
    'mecha': 'theme:mecha', 'giant robot': 'theme:mecha',
    'zombie': 'theme:zombie', 'undead': 'theme:zombie',
    'vampire': 'theme:vampire', 'werewolf': 'theme:werewolf', 'monster': 'theme:monster', 'kaiju': 'theme:kaiju', 
    'ghost': 'theme:ghost', 'haunting': 'theme:ghost', 'supernatural horror': 'theme:ghost',
    'slasher': 'theme:slasher', 'body horror': 'theme:body-horror', 'folk horror': 'theme:folk-horror',
    'magic': 'theme:magic', 'sword and sorcery': 'theme:magic',
    'gangster': 'theme:gangster', 'mafia': 'theme:gangster', 'mobster': 'theme:gangster',
    'heist': 'theme:heist', 'film-noir': 'theme:film-noir', 'hardboiled': 'theme:film-noir',
    'conspiracy': 'theme:conspiracy', 'spy': 'theme:spy', 'espionage': 'theme:spy', 'assassin': 'theme:assassin',
    'serial killer': 'theme:serial-killer', 'whodunit': 'theme:whodunit', 'courtroom drama': 'theme:courtroom',
    'wuxia': 'theme:wuxia', 'martial arts': 'theme:wuxia', 'kung fu': 'theme:wuxia',
    'xianxia': 'theme:xianxia', 'samurai': 'theme:samurai', 'ninja': 'theme:ninja', 'yakuza': 'theme:yakuza',
    'tokusatsu': 'theme:tokusatsu',
    'isekai': 'theme:isekai', 'slice of life': 'theme:slice-of-life', 'high school': 'theme:slice-of-life',
    'found footage': 'theme:found-footage',
};

// 类型门控
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

// 预编译正则表达式，使用单词边界 (\b)
const COMPILED_REGEX_MAP = {};
for (const [keyword, theme] of Object.entries(KEYWORD_TO_THEME_MAP)) {
    // 创建一个只匹配独立单词的正则表达式，忽略大小写
    COMPILED_REGEX_MAP[keyword] = {
        regex: new RegExp(`\\b${keyword.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&')}\\b`, 'i'),
        theme: theme
    };
}

// --- 语言映射 ---
const LANGUAGE_TO_COUNTRY = {
    // --- 亚洲 ---
    'zh': 'cn', // 中文 (Chinese) -> 中国 (China)
    'ja': 'jp', // 日语 (Japanese) -> 日本 (Japan)
    'ko': 'kr', // 韩语 (Korean) -> 韩国 (South Korea)
    'hi': 'in', // 印地语 (Hindi) -> 印度 (India) (宝莱坞)
    'th': 'th', // 泰语 (Thai) -> 泰国 (Thailand)
    'id': 'id', // 印度尼西亚语 (Indonesian) -> 印度尼西亚 (Indonesia)

    // --- 欧洲 ---
    'en': 'us', // 英语 (English) -> 美国 (USA) - 作为最大市场和制作国代表，也覆盖 GB/CA/AU 等
    'fr': 'fr', // 法语 (French) -> 法国 (France)
    'de': 'de', // 德语 (German) -> 德国 (Germany)
    'es': 'es', // 西班牙语 (Spanish) -> 西班牙 (Spain)
    'it': 'it', // 意大利语 (Italian) -> 意大利 (Italy)
    'ru': 'ru', // 俄语 (Russian) -> 俄罗斯 (Russia)
};



export function analyzeAndTagItem(item, imdbAkasInfo = { regions: new Set(), languages: new Set() }) {
    if (!item) return null;

    const tags = new Set();
    // --- 确定日期和年份 ---
    const release_date = item.release_date || item.first_air_date;
    const year = release_date ? new Date(release_date).getFullYear() : null;


    // 1. 基础类型标签 (type:movie, type:tv)
    const mediaType = item.media_type || (item.seasons ? 'tv' : 'movie');
    tags.add(`type:${mediaType}`);

    // 2. 类型标签 (genre:科幻, genre:动画) - 收集类型用于后续门控
    const genreNames = new Set();
    (item.genres || []).forEach(g => { if (g.name) genreNames.add(g.name); });
    genreNames.forEach(name => tags.add(`genre:${name}`));
    
    const isAnimation = genreNames.has('动画');
    if (isAnimation) {
        tags.add('type:animation');
    }

    // 3. 年代标签 (decade:1990s)
    if (year) {
        tags.add(`decade:${Math.floor(year / 10) * 10}s`);
    }

    // 4. 制作国家/地区/语言标签 (重大改进)
    const countries = new Set();
    
    // 4a. 添加 TMDB 原始国家/制作国家
    (item.origin_country || []).forEach(c => countries.add(c.toLowerCase()));
    (item.production_countries || []).forEach(pc => countries.add(pc.iso_3166_1.toLowerCase()));

    // 4b. 添加 IMDb AKAS 地区 
    imdbAkasInfo.regions.forEach(r => countries.add(r));

    // 4c. 添加从 IMDb AKAS 语言推断的国家
    imdbAkasInfo.languages.forEach(l => {
        const country = LANGUAGE_TO_COUNTRY[l];
        if (country) {
            countries.add(country);
        }
    });

    // 添加最终的国家标签
    countries.forEach(c => tags.add(`country:${c}`));

    // 5. 聚合地区标签 (基于合并后的国家信息和语言信息)
    if (countries.has('us') || countries.has('gb') || countries.has('fr') || countries.has('de') || countries.has('ca') || countries.has('au') || countries.has('it') || countries.has('es')) {
        tags.add('region:us-eu');
    }
    if (countries.has('jp') || countries.has('kr')) {
        tags.add('region:east-asia');
    }
    // 优先使用语言判断中文区，更准确
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

    // 7. 聚合分类标签 (不变)
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
    let finalMediaType = 'movie'; // 默认电影
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
        release_date: release_date, // 使用统一的完整日期
        release_year: year,
        vote_average: item.vote_average, // 对应客户端的 r
        vote_count: item.vote_count,
        popularity: item.popularity, // 对应客户端的 d (default_order)
        mediaType: finalMediaType, // 统一的类型: movie, tv, anime
        semantic_tags: Array.from(tags),
    };
}
