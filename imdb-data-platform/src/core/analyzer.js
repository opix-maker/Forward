// =================================================================================
// analyzer.js (v2.1 - DIAGNOSTIC MODE)
// =================================================================================

// --- 关键词到主题的映射 (优化结构) ---
const KEYWORD_TO_THEME_MAP = { 'theme:cyberpunk': ['cyberpunk', 'dystopia', 'virtual reality', 'artificial intelligence', 'neo-noir'], 'theme:space-opera': ['space opera', 'alien', 'galaxy', 'spaceship'], 'theme:time-travel': ['time travel', 'time loop', 'paradox'], 'theme:post-apocalyptic': ['post-apocalyptic', 'dystopian'], 'theme:superhero': ['superhero', 'marvel comics', 'dc comics', 'comic book'], 'theme:mecha': ['mecha', 'giant robot'], 'theme:zombie': ['zombie', 'undead'], 'theme:monster': ['vampire', 'werewolf', 'monster', 'kaiju'], 'theme:ghost': ['ghost', 'haunting', 'supernatural horror'], 'theme:slasher': ['slasher', 'body horror', 'folk horror'], 'theme:magic': ['magic', 'sword and sorcery'], 'theme:gangster': ['gangster', 'mafia', 'mobster', 'yakuza'], 'theme:heist': ['heist'], 'theme:film-noir': ['film-noir', 'hardboiled'], 'theme:spy': ['conspiracy', 'spy', 'espionage', 'assassin'], 'theme:serial-killer': ['serial killer'], 'theme:whodunit': ['whodunit'], 'theme:courtroom': ['courtroom drama'], 'theme:wuxia': ['wuxia', 'martial arts', 'kung fu'], 'theme:xianxia': ['xianxia'], 'theme:samurai': ['samurai', 'ninja'], 'theme:isekai': ['isekai'], 'theme:slice-of-life': ['slice of life', 'high school'], 'theme:found-footage': ['found footage'], };
const GATED_THEMES = { 'theme:ghost': ['恐怖', '惊悚', '悬疑'], 'theme:zombie': ['恐怖', '科幻'], 'theme:monster': ['恐怖', '科幻', '奇幻', '动作'], 'theme:slasher': ['恐怖', '惊悚'], 'theme:serial-killer': ['犯罪', '惊悚', '恐怖', '悬疑'], };
const COMPILED_THEME_REGEX = Object.entries(KEYWORD_TO_THEME_MAP).flatMap(([theme, keywords]) => keywords.map(keyword => ({ regex: new RegExp(`\\b${keyword.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&')}\\b`, 'i'), theme: theme })));
const GENRE_MAP_EN_TO_CN = { 'Action': '动作', 'Adventure': '冒险', 'Animation': '动画', 'Biography': '传记', 'Comedy': '喜剧', 'Crime': '犯罪', 'Documentary': '纪录片', 'Drama': '剧情', 'Family': '家庭', 'Fantasy': '奇幻', 'Film-Noir': '黑色电影', 'History': '历史', 'Horror': '恐怖', 'Music': '音乐', 'Musical': '歌舞', 'Mystery': '悬疑', 'Romance': '爱情', 'Sci-Fi': '科幻', 'Science Fiction': '科幻', 'Sport': '运动', 'Thriller': '惊悚', 'War': '战争', 'Western': '西部', 'Short': '短片', 'News': '新闻', 'Reality-TV': '真人秀', 'Talk-Show': '脱口秀', 'Game-Show': '游戏节目', };
const GENRE_MAP_CN_TO_CN = { '动作': '动作', '冒险': '冒险', '动画': '动画', '传记': '传记', '喜剧': '喜剧', '犯罪': '犯罪', '纪录片': '纪录片', '剧情': '剧情', '家庭': '家庭', '奇幻': '奇幻', '黑色电影': '黑色电影', '历史': '历史', '恐怖': '恐怖', '音乐': '音乐', '歌舞': '歌舞', '悬疑': '悬疑', '爱情': '爱情', '科幻': '科幻', '运动': '运动', '惊悚': '惊悚', '战争': '战争', '西部': '西部', '短片': '短片', '新闻': '新闻', '真人秀': '真人秀', '脱口秀': '脱口秀', '游戏节目': '游戏节目', };
const GENRE_NORMALIZER = { ...GENRE_MAP_EN_TO_CN, ...GENRE_MAP_CN_TO_CN };
function normalizeGenre(rawGenre) { if (!rawGenre) return null; let normalized = GENRE_NORMALIZER[rawGenre]; if (normalized) return normalized; const trimmed = rawGenre.trim(); const capitalized = trimmed.charAt(0).toUpperCase() + trimmed.slice(1).toLowerCase(); normalized = GENRE_NORMALIZER[capitalized] || GENRE_NORMALIZER[trimmed]; return normalized || null; }
const LANGUAGE_TO_COUNTRY = { 'zh': 'cn', 'ja': 'jp', 'ko': 'kr', 'en': 'us', 'fr': 'fr', 'de': 'de', 'es': 'es', 'it': 'it', };

export function analyzeAndTagItem(item, imdbAkasInfo = { regions: new Set(), languages: new Set() }, imdbGenresString = null) {
    if (!item) return null;
    const tags = new Set();
    const release_date = item.release_date || item.first_air_date;
    const year = release_date ? new Date(release_date).getFullYear() : null;
    const rawGenres = new Set();
    (item.genres || []).forEach(g => { if (g.name) rawGenres.add(g.name); });
    if (imdbGenresString && imdbGenresString !== '\\N') { imdbGenresString.split(',').forEach(g => rawGenres.add(g.trim())); }
    const standardizedGenres = new Set();
    rawGenres.forEach(raw => { const normalized = normalizeGenre(raw); if (normalized) { standardizedGenres.add(normalized); } });
    standardizedGenres.forEach(g => tags.add(`genre:${g}`));
    const isAnimation = standardizedGenres.has('动画');
    const isTV = item.media_type === 'tv' || !!item.seasons;
    const allCountries = new Set();
    const allLanguages = new Set();
    (item.origin_country || []).forEach(c => allCountries.add(c.toLowerCase()));
    (item.production_countries || []).forEach(pc => allCountries.add(pc.iso_3166_1.toLowerCase()));
    imdbAkasInfo.regions.forEach(r => allCountries.add(r));
    imdbAkasInfo.languages.forEach(l => allLanguages.add(l));
    if (item.original_language) allLanguages.add(item.original_language);
    allLanguages.forEach(l => { const country = LANGUAGE_TO_COUNTRY[l]; if (country) allCountries.add(country); });
    allCountries.forEach(c => tags.add(`country:${c}`));
    allLanguages.forEach(l => tags.add(`language:${l}`));
    if (['us', 'gb', 'fr', 'de', 'ca', 'au', 'it', 'es'].some(c => allCountries.has(c))) { tags.add('region:us-eu'); }
    if (['jp', 'kr'].some(c => allCountries.has(c))) { tags.add('region:east-asia'); }
    if (allLanguages.has('zh') || ['cn', 'hk', 'tw', 'sg', 'mo'].some(c => allCountries.has(c))) { tags.add('region:chinese'); }
    const tmdbKeywords = (item.keywords?.keywords || item.keywords?.results || []).map(k => k.name.toLowerCase());
    const foundThemes = new Set();
    tmdbKeywords.forEach(tmdbKeyword => { for (const { regex, theme } of COMPILED_THEME_REGEX) { if (regex.test(tmdbKeyword)) { foundThemes.add(theme); break; } } });
    foundThemes.forEach(theme => { const requiredGenres = GATED_THEMES[theme]; if (requiredGenres) { if (requiredGenres.some(g => standardizedGenres.has(g))) { tags.add(theme); } } else { tags.add(theme); } });
    let finalMediaType = 'movie';
    if (isAnimation && allCountries.has('jp')) { finalMediaType = 'anime'; } else if (isTV) { finalMediaType = 'tv'; }
    const chineseTranslation = item.translations?.translations?.find(t => t.iso_639_1 === 'zh' || t.iso_639_1 === 'zh-CN');
    const title_zh = chineseTranslation?.data?.title || chineseTranslation?.data?.name || item.title || item.name;
    const overview_zh = chineseTranslation?.data?.overview || item.overview;

    const analyzedItem = {
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

    // =============== DIAGNOSTIC LOGGING START ===============
    console.log(`\n-------------------- [ANALYZED ITEM for ${item.id}] --------------------`);
    console.log(JSON.stringify(analyzedItem, null, 2));
    console.log(`-------------------- [END ANALYZED ITEM for ${item.id}] --------------------\n`);
    // =============== DIAGNOSTIC LOGGING END ===============

    return analyzedItem;
}
