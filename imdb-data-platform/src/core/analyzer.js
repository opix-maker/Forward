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
    // 奖项
    'oscar (best picture)': 'award:oscar-winner', 'golden globe (best motion picture)': 'award:golden-globe-winner', 'palme d\'or': 'award:cannes-winner',
    // 情感/风格
    'tearjerker': 'mood:tearjerker', 'sadness': 'mood:tearjerker',
    'feel-good': 'mood:feel-good', 'uplifting': 'mood:feel-good',
    'suspense': 'mood:suspenseful', 'thrilling': 'mood:suspenseful',
    'visually stunning': 'style:visual-spectacle', 'epic': 'style:visual-spectacle',
    'plot twist': 'style:plot-twist', 'mind-bender': 'style:plot-twist',
};

export function analyzeAndTagItem(item) {
    if (!item) return null;

    const tags = new Set();
    const year = item.release_date ? new Date(item.release_date).getFullYear() : (item.first_air_date ? new Date(item.first_air_date).getFullYear() : null);

    // 1. 基础类型标签 (type:movie, type:tv)
    const mediaType = item.media_type || (item.seasons ? 'tv' : 'movie');
    tags.add(`type:${mediaType}`);

    // 2. 类型标签 (genre:科幻, genre:动画)
    const genreNames = new Set();
    (item.genres || []).forEach(g => { if (g.name) genreNames.add(g.name); });
    genreNames.forEach(name => tags.add(`genre:${name}`));
    if (genreNames.has('动画')) {
        tags.add('type:animation');
    }

    // 3. 年代标签 (decade:1990s)
    if (year) {
        tags.add(`decade:${Math.floor(year / 10) * 10}s`);
    }

    // 4. 制作国家/地区标签 (country:us, country:jp)
    const countries = new Set();
    (item.origin_country || []).forEach(c => countries.add(c.toLowerCase()));
    (item.production_countries || []).forEach(pc => countries.add(pc.iso_3166_1.toLowerCase()));
    countries.forEach(c => tags.add(`country:${c}`));

    // 5. 聚合地区标签 (region:us-eu, region:east-asia, region:chinese)
    if (countries.has('us') || countries.has('gb') || countries.has('fr') || countries.has('de')) tags.add('region:us-eu');
    if (countries.has('jp') || countries.has('kr')) tags.add('region:east-asia');
    if (countries.has('cn') || countries.has('hk') || countries.has('tw')) tags.add('region:chinese');

    // 6. 关键词/主题/风格/情绪标签
    const keywords = (item.keywords?.keywords || item.keywords?.results || []).map(k => k.name.toLowerCase());
    keywords.forEach(keyword => {
        for (const [mapKey, theme] of Object.entries(KEYWORD_TO_THEME_MAP)) {
            if (keyword.includes(mapKey)) tags.add(theme);
        }
    });
    
    // 7. 聚合分类标签 (category:us_tv, category:jp_anime)
    if (tags.has('type:tv')) {
        if (tags.has('region:us-eu')) tags.add('category:us-eu_tv');
        if (tags.has('region:east-asia')) tags.add('category:east-asia_tv');
        if (tags.has('region:chinese')) tags.add('category:chinese_tv');
    }
    if (tags.has('type:animation')) {
        if (tags.has('country:jp')) tags.add('category:jp_anime');
        if (tags.has('country:cn')) tags.add('category:cn_anime');
    }

    const chineseTranslation = item.translations?.translations?.find(t => t.iso_639_1 === 'zh');
    const title_zh = chineseTranslation?.data?.title || chineseTranslation?.data?.name || item.title || item.name;
    const overview_zh = chineseTranslation?.data?.overview || item.overview;

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
        belongs_to_collection: item.belongs_to_collection,
        semantic_tags: Array.from(tags),
    };
}
