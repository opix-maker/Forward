// --- 语义标签词典 ---
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
    'vampire': 'theme:vampire', 'werewolf': 'theme:werewolf', 'monster': 'theme:monster',
    'ghost': 'theme:ghost', 'haunting': 'theme:ghost', 'supernatural horror': 'theme:ghost',
    'found footage': 'theme:found-footage', 'slasher': 'theme:slasher', 'body horror': 'theme:body-horror', 'folk horror': 'theme:folk-horror',
    'magic': 'theme:magic', 'sword and sorcery': 'theme:magic',
    // 犯罪/悬疑
    'gangster': 'theme:gangster', 'mafia': 'theme:gangster', 'mobster': 'theme:gangster',
    'heist': 'theme:heist', 'film-noir': 'theme:film-noir', 'hardboiled': 'theme:film-noir',
    'conspiracy': 'theme:conspiracy', 'spy': 'theme:spy', 'espionage': 'theme:spy', 'assassin': 'theme:assassin',
    'serial killer': 'theme:serial-killer', 'whodunit': 'theme:whodunit', 'courtroom drama': 'theme:courtroom',
    // 亚洲文化
    'wuxia': 'theme:wuxia', 'martial arts': 'theme:wuxia', 'kung fu': 'theme:wuxia',
    'xianxia': 'theme:xianxia', 'samurai': 'theme:samurai', 'ninja': 'theme:ninja', 'yakuza': 'theme:yakuza',
    'kaiju': 'theme:kaiju', 'tokusatsu': 'theme:tokusatsu',
    'isekai': 'theme:isekai', 'slice of life': 'theme:slice-of-life', 'high school': 'theme:slice-of-life',
    // 成人内容
    'erotic thriller': 'theme:adult', 'erotic drama': 'theme:adult', 'softcore': 'theme:adult', 'sex': 'theme:adult', 'nudity': 'theme:adult', 'adult animation': 'theme:adult-animation',
    // 其他
    'coming of age': 'theme:coming-of-age', 'road movie': 'theme:road-movie', 'mockumentary': 'theme:mockumentary', 'musical': 'theme:musical', 'satire': 'theme:satire', 'biopic': 'theme:biopic',
};

export function analyzeAndTagItem(item) {
    if (!item) return null;

    const tags = new Set();
    const year = item.release_date ? new Date(item.release_date).getFullYear() : (item.first_air_date ? new Date(item.first_air_date).getFullYear() : null);

    // 1. 基础类型标签 (type:movie, type:tv)
    if (item.media_type) tags.add(`type:${item.media_type}`);

    // 2. 类型标签 (genre:科幻, genre:动画)
    (item.genres || []).forEach(g => {
        if (g.name) tags.add(`genre:${g.name}`);
    });
    // 特殊处理动画
    if (item.genres?.some(g => g.id === 16)) {
        tags.add('type:animation');
    }

    // 3. 年代标签 (decade:1990s)
    if (year) {
        const decade = Math.floor(year / 10) * 10;
        tags.add(`decade:${decade}s`);
    }

    // 4. 制作国家/地区标签 (country:us, country:jp)
    const countries = new Set();
    (item.origin_country || []).forEach(c => countries.add(c.toLowerCase()));
    (item.production_countries || []).forEach(pc => countries.add(pc.iso_3166_1.toLowerCase()));
    countries.forEach(c => tags.add(`country:${c}`));

    // 5. 原始语言标签 (lang:en, lang:ja, lang:chinese)
    if (item.original_language) {
        const lang = item.original_language;
        tags.add(`lang:${lang}`);
        if (['zh', 'cmn', 'yue'].includes(lang)) {
            tags.add('lang:chinese');
        }
    }

    // 6. 主题标签 (theme:cyberpunk)
    const keywords = (item.keywords?.keywords || item.keywords?.results || []).map(k => k.name.toLowerCase());
    keywords.forEach(keyword => {
        for (const [mapKey, theme] of Object.entries(KEYWORD_TO_THEME_MAP)) {
            if (keyword.includes(mapKey)) {
                tags.add(theme);
            }
        }
    });
    
    // 7. 成人内容标签
    if (item.adult) tags.add('theme:adult');

    // 提取中文标题和简介
    const chineseTranslation = item.translations?.translations?.find(t => t.iso_639_1 === 'zh');
    const title_zh = chineseTranslation?.data?.title || chineseTranslation?.data?.name || item.title || item.name;
    const overview_zh = chineseTranslation?.data?.overview || item.overview;

    // 返回一个干净、扁平化的对象
    return {
        id: item.id,
        imdb_id: item.external_ids?.imdb_id,
        title: title_zh,
        overview: overview_zh,
        poster_path: item.poster_path,
        backdrop_path: item.backdrop_path,
        release_date: item.release_date || item.first_air_date,
        release_year: year,
        vote_average: item.vote_average,
        vote_count: item.vote_count,
        popularity: item.popularity,
        semantic_tags: Array.from(tags),
    };
}
