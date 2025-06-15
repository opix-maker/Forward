import fetch from 'node-fetch';

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const TMDB_ACCESS_TOKEN_V4 = process.env.TMDB_ACCESS_TOKEN_V4;
const API_BASE_URL = 'https://api.themoviedb.org/3';
const MAX_RETRIES = 3; 
const RETRY_DELAY_BASE = 1500;

if (!TMDB_ACCESS_TOKEN_V4) {
    throw new Error('FATAL: TMDB_ACCESS_TOKEN_V4 environment variable is not set!');
}

async function fetchWithRetry(url) {
    const options = {
        method: 'GET',
        headers: {
            accept: 'application/json',
            Authorization: `Bearer ${TMDB_ACCESS_TOKEN_V4}`
        }
    };

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
            const response = await fetch(url, options);
            if (response.ok || (response.status >= 400 && response.status < 500 && response.status !== 429)) {
                return response;
            }
            if (response.status === 429 || response.status >= 500) {
                if (attempt < MAX_RETRIES) {
                    const retryAfterHeader = response.headers.get('Retry-After');
                    const delay = retryAfterHeader ? (parseInt(retryAfterHeader, 10) * 1000 + 500) : (RETRY_DELAY_BASE * attempt);
                    console.warn(`  TMDB API Retryable error (${response.status}) for ${url}. Retrying in ${delay / 1000}s...`);
                    await sleep(delay);
                    continue;
                }
            }
            return response; // Return failed response after max retries or for non-retryable errors
        } catch (networkError) {
            console.warn(`  Network error on attempt ${attempt}: ${networkError.message}`);
            if (attempt >= MAX_RETRIES) {
                throw new Error(`TMDB Network Error after ${MAX_RETRIES} attempts for ${url}: ${networkError.message}`);
            }
            await sleep(RETRY_DELAY_BASE * attempt);
        }
    }
    throw new Error(`Failed to fetch from TMDB after multiple attempts: ${url}`);
}



export async function getFullDetailsByImdbId(imdbId) {
    // --- Step 1: Find the TMDB ID and media type from IMDb ID ---
    const findUrl = new URL(`${API_BASE_URL}/find/${imdbId}`);
    findUrl.searchParams.append('external_source', 'imdb_id');
    
    const findResponse = await fetchWithRetry(findUrl.toString());
    if (!findResponse.ok) return null;

    const findData = await findResponse.json();
    const result = findData.movie_results?.[0] || findData.tv_results?.[0];
    
    if (!result || !result.id) {
        return null;
    }
    
    const tmdbId = result.id;
    const mediaType = findData.movie_results?.length > 0 ? 'movie' : 'tv';

    // --- Step 2: Get the FULL details using the found TMDB ID ---
    const detailsUrl = new URL(`${API_BASE_URL}/${mediaType}/${tmdbId}`);
    detailsUrl.searchParams.append('language', 'zh-CN');
    detailsUrl.searchParams.append('append_to_response', 'keywords,translations,external_ids');

    const detailsResponse = await fetchWithRetry(detailsUrl.toString());
    if (!detailsResponse.ok) {
        // 如果中文请求失败 (e.g., 404), 尝试用英文回退，确保能拿到基础数据
        console.warn(`  Could not fetch Chinese details for ${imdbId} (${tmdbId}). Falling back to English.`);
        const fallbackUrl = new URL(`${API_BASE_URL}/${mediaType}/${tmdbId}`);
        fallbackUrl.searchParams.append('append_to_response', 'keywords,translations,external_ids');
        const fallbackResponse = await fetchWithRetry(fallbackUrl.toString());
        if (!fallbackResponse.ok) return null;
        return fallbackResponse.json();
    }
    
    return detailsResponse.json();
}
