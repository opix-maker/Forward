// --- 关键词到主题的映射  ---
const KEYWORD_TO_THEME_MAP = { 'theme:cyberpunk': ['cyberpunk', 'dystopia', 'virtual reality', 'artificial intelligence', 'neo-noir'], 'theme:space-opera': ['space opera', 'alien', 'galaxy', 'spaceship'], 'theme:time-travel': ['time travel', 'time loop', 'paradox'], 'theme:post-apocalyptic': ['post-apocalyptic', 'dystopian'], 'theme:superhero': ['superhero', 'marvel comics', 'dc comics', 'comic book'], 'theme:mecha': ['mecha', 'giant robot'], 'theme:zombie': ['zombie', 'undead'], 'theme:monster': ['vampire', 'werewolf', 'monster', 'kaiju'], 'theme:ghost': ['ghost', 'haunting', 'supernatural horror'], 'theme:slasher': ['slasher', 'body horror', 'folk horror'], 'theme:magic': ['magic', 'sword and sorcery'], 'theme:gangster': ['gangster', 'mafia', 'mobster', 'yakuza'], 'theme:heist': ['heist'], 'theme:film-noir': ['film-noir', 'hardboiled'], 'theme:spy': ['conspiracy', 'spy', 'espionage', 'assassin'], 'theme:serial-killer': ['serial killer'], 'theme:whodunit': ['whodunit'], 'theme:courtroom': ['courtroom drama'], 'theme:wuxia': ['wuxia', 'martial arts', 'kung fu'], 'theme:xianxia': ['xianxia'], 'theme:samurai': ['samurai', 'ninja'], 'theme:isekai': ['isekai'], 'theme:slice-of-life': ['slice of life', 'high school'], 'theme:found-footage': ['found footage'], };
const GATED_THEMES = { 'theme:ghost': ['恐怖', '惊悚', '悬疑'], 'theme:zombie': ['恐怖', '科幻'], 'theme:monster': ['恐怖', '科幻', '奇幻', '动作'], 'theme:slasher': ['恐怖', '惊悚'], 'theme:serial-killer': ['犯罪', '惊悚', '恐怖', '悬疑'], };
const COMPILED_THEME_REGEX = Object.entries(KEYWORD_TO_THEME_MAP).flatMap(([theme, keywords]) => keywords.map(keyword => ({ regex: new RegExp(`\\b${keyword.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&')}\\b`, 'i'), theme: theme })));

// --- IMDb 英文类型到中文的映射 ---
const IMDB_GENRE_TO_CHINESE = {
    'Action': '动作', 'Adventure': '冒险', 'Animation': '动画', 'Biography': '传记',
    'Comedy': '喜剧', 'Crime': '犯罪', 'Documentary': '纪录片', 'Drama': '剧情',
    'Family': '家庭', 'Fantasy': '奇幻', 'Film-Noir': '黑色电影', 'History': '历史',
    'Horror': '恐怖', 'Music': '音乐', 'Musical': '歌舞', 'Mystery': '悬疑',
    'Romance': '爱情', 'Sci-Fi': '科幻', 'Sport': '运动', 'Thriller': '惊悚',
    'War': '战争', 'Western': '西部', 'Short': '短片', 'News': '新闻',
    'Reality-TV': '真人秀', 'Talk-Show': '脱口秀', 'Game-Show': '游戏节目'
};

const LANGUAGE_TO_COUNTRY = { 'zh': 'cn', 'ja': 'jp', 'ko': 'kr', 'en': 'us', 'fr': 'fr', 'de': 'de', 'es': 'es', 'it': 'it', };



export function analyzeAndTagItem(tmdbDetails, imdbAkasInfo = { regions: new Set(), languages: new Set() }, imdbGenresString = null) {
    if (!tmdbDetails) return null;

    const tags = new Set();
    const release_date = tmdbDetails.release_date || tmdbDetails.first_air_date;
    const year = release_date ? new Date(release_date).getFullYear() : null;

    // --- 1. 类型处理  ---
    const standardizedGenres = new Set();
    if (imdbGenresString && imdbGenresString !== '\\N') {
        const imdbGenres = imdbGenresString.split(',');
        imdbGenres.forEach(genreEn => {
            const genreCn = IMDB_GENRE_TO_CHINESE[genreEn.trim()];
            if (genreCn) {
                standardizedGenres.add(genreCn);
            }
        });
    }
    standardizedGenres.forEach(g => tags.add(`genre:${g}`));
    
    const isAnimation = standardizedGenres.has('动画');
    const isTV = tmdbDetails.media_type === 'tv' || !!tmdbDetails.seasons;

    // --- 2. 地区与语言处理 ---
    const allCountries = new Set(imdbAkasInfo.regions);
    const allLanguages = new Set(imdbAkasInfo.languages);
    
    allLanguages.forEach(l => {
        const country = LANGUAGE_TO_COUNTRY[l];
        if (country) allCountries.add(country);
    });

    allCountries.forEach(c => tags.add(`country:${c}`));
    allLanguages.forEach(l => tags.add(`language:${l}`));
    
    if (['us', 'gb', 'fr', 'de', 'ca', 'au', 'it', 'es'].some(c => allCountries.has(c))) {
        tags.add('region:us-eu');
    }
    if (['jp', 'kr'].some(c => allCountries.has(c))) {
        tags.add('region:east-asia');
    }
    if (allLanguages.has('zh') || ['cn', 'hk', 'tw', 'sg', 'mo'].some(c => allCountries.has(c))) {
        tags.add('region:chinese');
    }

    // --- 3. 主题处理 ---
    const tmdbKeywords = (tmdbDetails.keywords?.keywords || tmdbDetails.keywords?.results || []).map(k => k.name.toLowerCase());
    const foundThemes = new Set();
    tmdbKeywords.forEach(tmdbKeyword => { for (const { regex, theme } of COMPILED_THEME_REGEX) { if (regex.test(tmdbKeyword)) { foundThemes.add(theme); break; } } });
    foundThemes.forEach(theme => { const requiredGenres = GATED_THEMES[theme]; if (requiredGenres) { if (requiredGenres.some(g => standardizedGenres.has(g))) { tags.add(theme); } } else { tags.add(theme); } });

    // --- 4. 最终分类与数据整理 ---
    let finalMediaType = 'movie';
    if (isAnimation && allCountries.has('jp')) {
        finalMediaType = 'anime';
    } else if (isTV) {
        finalMediaType = 'tv';
    }
    
    return {
        id: tmdbDetails.id,
        imdb_id: tmdbDetails.imdb_id, // 从TMDB补充
        title: tmdbDetails.title || tmdbDetails.name,
        overview: tmdbDetails.overview,
        poster_path: tmdbDetails.poster_path,
        backdrop_path: tmdbDetails.backdrop_path,
        release_date: release_date,
        release_year: year,
        vote_average: tmdbDetails.vote_average,
        vote_count: tmdbDetails.vote_count,
        popularity: tmdbDetails.popularity,
        mediaType: finalMediaType,
        semantic_tags: Array.from(tags),
    };
}
