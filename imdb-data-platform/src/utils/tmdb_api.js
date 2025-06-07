import fetch from 'node-fetch';

const TMDB_ACCESS_TOKEN_V4 = process.env.TMDB_ACCESS_TOKEN_V4;
const API_BASE_URL = 'https://api.themoviedb.org/3';

if (!TMDB_ACCESS_TOKEN_V4) {
    throw new Error('TMDB_ACCESS_TOKEN_V4 environment variable is not set!');
}

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
        if (response.status === 404) {
            console.warn(`  TMDB 404 Not Found for endpoint: ${endpoint}`);
            return null;
        }
        const errorData = await response.text();
        throw new Error(`TMDB API Error for ${endpoint}: ${response.status} - ${errorData}`);
    }
    return response.json();
}

export async function findByImdbId(imdbId) {
    try {
        const data = await fetchFromTmdb(`/find/${imdbId}`, { external_source: 'imdb_id' });
        const result = data.movie_results?.[0] || data.tv_results?.[0];
        return result || null;
    } catch (error) {
        console.error(`  Error finding TMDB data for IMDb ID ${imdbId}:`, error.message);
        return null;
    }
}

export async function getTmdbDetails(tmdbId, mediaType = 'movie') {
    try {
        return await fetchFromTmdb(`/${mediaType}/${tmdbId}`, {
            language: 'zh-CN',
            append_to_response: 'credits,keywords,translations,external_ids' // 增加keywords和external_ids
        });
    } catch (error) {
        console.error(`  Error fetching details for TMDB ID ${tmdbId}:`, error.message);
        return null;
    }
}
