// 定义关键词到语义标签的映射
const KEYWORD_TO_THEME_MAP = {
    'cyberpunk': 'theme:cyberpunk',
    'dystopia': 'theme:dystopian',
    'post-apocalyptic': 'theme:post-apocalyptic',
    'zombie': 'theme:zombie',
    'vampire': 'theme:vampire',
    'gangster': 'theme:gangster',
    'film-noir': 'theme:film-noir',
    'neo-noir': 'theme:neo-noir',
    'space-opera': 'theme:space-opera',
    'time-travel': 'theme:time-travel',
    'mind-bender': 'theme:mind-bender',
    'superhero': 'theme:superhero',
    'martial-arts': 'theme:wuxia',
    'wuxia': 'theme:wuxia',
    'isekai': 'theme:isekai',
    'slice-of-life': 'theme:slice-of-life',
    'mecha': 'theme:mecha',
    'historical-drama': 'theme:historical-drama',
    'erotic-thriller': 'theme:adult',
    'softcore': 'theme:adult',
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
    (item.genres || []).forEach(g => tags.add(`genre:${g.name.toLowerCase().replace(' ', '-')}`));
    
    // 2. 年代标签
    if (item.release_date) {
        const year = new Date(item.release_date).getFullYear();
        if (year) {
            const decade = Math.floor(year / 10) * 10;
            tags.add(`decade:${decade}s`);
        }
    }

    // 3. 制作国家/地区标签
    (item.origin_country || []).forEach(c => tags.add(`country:${c.toLowerCase()}`));
    
    // 4. 原始语言标签
    if (item.original_language) tags.add(`lang:${item.original_language}`);

    // 5. 系列电影标签 (来自TMDB的官方数据)
    if (item.belongs_to_collection) {
        tags.add(`series:${item.belongs_to_collection.id}`);
    }

    // 6. 关键词/主题标签
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
    return item;
}
