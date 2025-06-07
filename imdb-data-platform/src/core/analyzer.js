

// --- 语义标签词典 (极致扩充) ---
const KEYWORD_TO_THEME_MAP = {
    // --- 核心科幻主题 ---
    'cyberpunk': 'theme:cyberpunk',
    'dystopia': 'theme:cyberpunk', // 赛博朋克经常是反乌托邦
    'dystopian future': 'theme:cyberpunk',
    'virtual reality': 'theme:cyberpunk',
    'artificial intelligence': 'theme:cyberpunk',
    'neo-noir': 'theme:cyberpunk',
    'space opera': 'theme:space-opera',
    'alien': 'theme:space-opera',
    'galaxy': 'theme:space-opera',
    'spaceship': 'theme:space-opera',
    'time travel': 'theme:time-travel',
    'time loop': 'theme:time-travel',
    'paradox': 'theme:time-travel',
    'post-apocalyptic': 'theme:post-apocalyptic',
    'dystopian': 'theme:post-apocalyptic', // 反乌托邦也可是末世
    'superhero': 'theme:superhero',
    'marvel comics': 'theme:superhero',
    'dc comics': 'theme:superhero',
    'comic book': 'theme:superhero',
    'mecha': 'theme:mecha',
    'giant robot': 'theme:mecha',

    // --- 核心奇幻/恐怖主题 ---
    'zombie': 'theme:zombie',
    'undead': 'theme:zombie',
    'vampire': 'theme:vampire',
    'werewolf': 'theme:werewolf',
    'monster': 'theme:monster',
    'ghost': 'theme:ghost',
    'haunting': 'theme:ghost',
    'supernatural horror': 'theme:ghost',
    'found footage': 'theme:found-footage',
    'slasher': 'theme:slasher',
    'body horror': 'theme:body-horror',
    'folk horror': 'theme:folk-horror',
    'magic': 'theme:magic',
    'sword and sorcery': 'theme:magic',

    // --- 犯罪/悬疑/惊悚主题 ---
    'gangster': 'theme:gangster',
    'mafia': 'theme:gangster',
    'mobster': 'theme:gangster',
    'heist': 'theme:heist',
    'film-noir': 'theme:film-noir',
    'hardboiled': 'theme:film-noir',
    'conspiracy': 'theme:conspiracy',
    'spy': 'theme:spy',
    'espionage': 'theme:spy',
    'assassin': 'theme:assassin',
    'serial killer': 'theme:serial-killer',
    'whodunit': 'theme:whodunit',
    'courtroom drama': 'theme:courtroom',
    'legal drama': 'theme:courtroom',

    // --- 亚洲文化主题 ---
    'wuxia': 'theme:wuxia',
    'martial arts': 'theme:wuxia',
    'kung fu': 'theme:wuxia',
    'xianxia': 'theme:xianxia',
    'samurai': 'theme:samurai',
    'ninja': 'theme:ninja',
    'yakuza': 'theme:yakuza',
    'kaiju': 'theme:kaiju', // 怪兽
    'tokusatsu': 'theme:tokusatsu', // 特摄
    'isekai': 'theme:isekai',
    'slice of life': 'theme:slice-of-life',
    'high school': 'theme:slice-of-life',
    'historical drama': 'theme:historical-drama',
    
    // --- 成人内容主题 ---
    'erotic thriller': 'theme:adult',
    'erotic drama': 'theme:adult',
    'softcore': 'theme:adult',
    'sex': 'theme:adult',
    'nudity': 'theme:adult',
    'adult animation': 'theme:adult-animation',

    // --- 其他风格/流派 ---
    'coming of age': 'theme:coming-of-age',
    'road movie': 'theme:road-movie',
    'mockumentary': 'theme:mockumentary',
    'musical': 'theme:musical',
    'satire': 'theme:satire',
    'biopic': 'theme:biopic',
    'based on a true story': 'theme:biopic',
};

/**
 * 分析单个TMDB条目，并为其附加一系列语义标签
 * @param {object} item - 从TMDB获取的完整详情对象
 * @returns {object} - 附加了 `semantic_tags` 数组的条目对象
 */
export function analyzeAndTagItem(item) {
    if (!item) return null;

    const tags = new Set();

    // 1. 基本类型标签
    if (item.media_type) tags.add(`type:${item.media_type}`);
    (item.genres || []).forEach(g => tags.add(`genre:${g.name.toLowerCase().replace(/ /g, '-')}`));
    
    // 2. 年代标签
    if (item.release_date || item.first_air_date) {
        const year = new Date(item.release_date || item.first_air_date).getFullYear();
        if (year) {
            const decade = Math.floor(year / 10) * 10;
            tags.add(`decade:${decade}s`);
        }
    }

    // 3. 制作国家/地区标签 (增强版)
    const countries = new Set();
    (item.origin_country || []).forEach(c => countries.add(c.toLowerCase()));
    (item.production_countries || []).forEach(pc => countries.add(pc.iso_3166_1.toLowerCase()));
    countries.forEach(c => tags.add(`country:${c}`));

    // 4. 原始语言标签 (增强版)
    if (item.original_language) {
        const lang = item.original_language;
        tags.add(`lang:${lang}`);
        // 聚合中文标签
        if (lang === 'zh' || lang === 'cmn' || lang === 'yue') {
            tags.add('lang:chinese');
        }
    }

    // 5. 系列电影标签
    if (item.belongs_to_collection) {
        tags.add(`series:${item.belongs_to_collection.id}`);
    }

    // 6. 关键词/主题标签 (使用扩充词典)
    const keywords = (item.keywords?.keywords || item.keywords?.results || []).map(k => k.name.toLowerCase());
    keywords.forEach(keyword => {
        for (const [mapKey, theme] of Object.entries(KEYWORD_TO_THEME_MAP)) {
            if (keyword.includes(mapKey)) {
                tags.add(theme);
            }
        }
    });

    // 7. 成人内容标签
    if (item.adult) {
        tags.add('theme:adult');
    }

    item.semantic_tags = Array.from(tags);
    
    // 增加调试日志
    if (item.semantic_tags.some(t => t.startsWith('theme:'))) {
        console.log(`    [Analyzer] Tagged "${item.title || item.name}" with themes: ${item.semantic_tags.filter(t => t.startsWith('theme:')).join(', ')}`);
    }

    return item;
}
