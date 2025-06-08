import fetch from 'node-fetch';

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));


const TMDB_ACCESS_TOKEN_V4 = process.env.TMDB_ACCESS_TOKEN_V4;
const API_BASE_URL = 'https://api.themoviedb.org/3';
const MAX_RETRIES = 3; 
const RETRY_DELAY_BASE = 1500; // ms

if (!TMDB_ACCESS_TOKEN_V4) {
    throw new Error('TMDB_ACCESS_TOKEN_V4 environment variable is not set!');
}

 // --- Fetch with Retry Logic ---
async function fetchWithRetry(url, options) {
     for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
            const response = await fetch(url, options);

            // Success or definitive errors (404, auth error etc.) - return immediately
             if (response.ok || (response.status >= 400 && response.status < 500 && response.status !== 429) ) {
                 return response;
             }
             
             // Handle retryable errors: 429 (Rate Limit) or 5xx (Server Error)
             if (response.status === 429 || response.status >= 500) {
                  if (attempt < MAX_RETRIES) {
                     // Check Retry-After header or use exponential backoff
                     const retryAfterHeader = response.headers.get('Retry-After');
                     const delay = retryAfterHeader 
                                    ? (parseInt(retryAfterHeader, 10) * 1000 + 500) // Respect header + buffer
                                    : (RETRY_DELAY_BASE * attempt); // Exponential backoff
                     console.warn(`  TMDB API Retryable error (${response.status}) on ${url.pathname}. Attempt ${attempt}/${MAX_RETRIES}. Retrying in ${delay / 1000}s...`);
                     await sleep(delay);
                     continue; // Retry
                 } else {
                     // Max retries reached
                     console.error(`  TMDB API Max retries reached for ${url.pathname}. Status: ${response.status}`);
                      return response; // Return the failed response
                 }
             }
              // Other non-ok status
             return response;

        } catch (networkError) {
             console.warn(`  Network error on attempt ${attempt}: ${networkError.message}`);
              if (attempt < MAX_RETRIES) {
                 await sleep(RETRY_DELAY_BASE * attempt);
              } else {
                   throw new Error(`TMDB Network Error after ${MAX_RETRIES} attempts for ${url}: ${networkError.message}`);
              }
        }
     }
      // Should ideally not be reached if MAX_RETRIES > 0
      throw new Error(`Failed to fetch from TMDB after multiple attempts: ${url}`);
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
	
	// Use fetchWithRetry
    const response = await fetchWithRetry(url, options); 

    if (!response.ok) {
        if (response.status === 404) {
           // console.warn(`  TMDB 404 Not Found for endpoint: ${endpoint}`); // Reduce log noise
            return null;
        }
         // Log error body for better debugging
         const errorBody = await response.text().catch(() => 'Could not read error body');
         console.error(`TMDB API Error Response for ${endpoint}: Status ${response.status} - Body: ${errorBody}`);
        // Throw error to be caught by caller, allowing it to skip the item
        throw new Error(`TMDB API Error for ${endpoint}: ${response.status} - ${response.statusText}`);
    }
    return response.json();
}

export async function findByImdbId(imdbId) {
    // Error is caught in build.js enrichmentTask
    const data = await fetchFromTmdb(`/find/${imdbId}`, { external_source: 'imdb_id' });
     if (!data) return null;
    const result = data.movie_results?.[0] || data.tv_results?.[0];
    // Add media_type if not present for consistency
     if(result && !result.media_type){
       result.media_type = data.movie_results?.[0] ? 'movie' : (data.tv_results?.[0] ? 'tv' : undefined);
    }
    return result || null;
}

export async function getTmdbDetails(tmdbId, mediaType = 'movie') {
     // Handle cases where mediaType might be undefined from findByImdbId
    if(!mediaType || !['movie', 'tv'].includes(mediaType)) {
       // console.warn(` Invalid media type "${mediaType}" for TMDB ID ${tmdbId}, skipping details fetch.`);
        return null;
    }
     // Error is caught in build.js enrichmentTask
     return await fetchFromTmdb(`/${mediaType}/${tmdbId}`, {
            language: 'zh-CN',
            append_to_response: 'credits,keywords,translations,external_ids'
     });
}
