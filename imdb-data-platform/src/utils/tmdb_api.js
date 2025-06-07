import fetch from 'node-fetch';

const TMDB_ACCESS_TOKEN_V4 = process.env.TMDB_ACCESS_TOKEN_V4;
const API_BASE_URL = 'https://api.themoviedb.org/3';

if (!TMDB_ACCESS_TOKEN_V4) {
    throw new Error('TMDB_ACCESS_TOKEN_V4 environment variable is not set!');
}

// 封装的API请求函数
async function fetchFromTmdb(endpoint, params = {}) {
    const url = new URL(`${API_BASE_URL}${endpoint}`);
    Object.keys(params).forEach(key => url.searchParams.append(key, params[key]));

    const options = {
        method: 'GET',
        headers: {
            accept: 'application/json',
            Authorization: `Bearer ${TMDB_ACCESS_TOKEN_V4}`
        }
    };

    const response = await fetch(url, options);
    if (!response.ok) {
        const errorData = await response.text();
        throw new Error(`TMDB API Error for ${endpoint}: ${response.status} - ${errorData}`);
    }
    return response.json();
}

/**
 * 通过IMDb ID精确查找TMDB数据
 * @param {string} imdbId - IMDb ID (e.g., "tt0111161")
 * @returns {Promise<object|null>} - TMDB电影对象或null
 */
export async function findByImdbId(imdbId) {
    try {
        const data = await fetchFromTmdb(`/find/${imdbId}`, { external_source: 'imdb_id' });
        // /find 端点返回一个包含不同类型结果的对象
        if (data.movie_results && data.movie_results.length > 0) {
            return data.movie_results[0];
        }
        if (data.tv_results && data.tv_results.length > 0) {
            return data.tv_results[0];
        }
        return null;
    } catch (error) {
        console.error(`  Error finding TMDB data for IMDb ID ${imdbId}:`, error.message);
        return null;
    }
}

/**
 * 获取一个TMDB电影的完整详情，包括演职员、关键词等
 * @param {number} tmdbId - TMDB电影ID
 * @returns {Promise<object|null>}
 */
export async function getTmdbDetails(tmdbId, mediaType = 'movie') {
    try {
        return await fetchFromTmdb(`/${mediaType}/${tmdbId}`, {
            append_to_response: 'credits,keywords,translations'
        });
    } catch (error) {
        console.error(`  Error fetching details for TMDB ID ${tmdbId}:`, error.message);
        return null;
    }
}
