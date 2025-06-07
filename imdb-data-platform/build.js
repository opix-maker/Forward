import fs from 'fs/promises';
import { buildImdbData } from './src/builders/imdb_builder.js';
import { findByImdbId, getTmdbDetails } from './src/utils/tmdb_api.js';
import { sleep } from './src/utils/helpers.js';

const MAX_CONCURRENT_ENRICHMENTS = 10;

async function enrichDataWithTmdb(rawImdbData) {
    console.log('Enriching raw IMDb data with TMDB details...');
    const enrichedData = {};

    for (const [chartKey, regions] of Object.entries(rawImdbData)) {
        enrichedData[chartKey] = {};
        console.log(`- Enriching chart type: ${chartKey}`);

        for (const [regionKey, items] of Object.entries(regions)) {
            console.log(`  - Enriching region: ${regionKey} (${items.length} items)`);
            const enrichedItems = [];
            
            for (let i = 0; i < items.length; i += MAX_CONCURRENT_ENRICHMENTS) {
                const batch = items.slice(i, i + MAX_CONCURRENT_ENRICHMENTS);
                const promises = batch.map(async (item) => {
                    const tmdbSearchResult = await findByImdbId(item.imdbId);
                    if (!tmdbSearchResult) return null;

                    const mediaType = tmdbSearchResult.media_type || 'movie';
                    const tmdbDetails = await getTmdbDetails(tmdbSearchResult.id, mediaType);
                    if (!tmdbDetails) return null;
                    
                    return {
                        imdb_id: item.imdbId,
                        imdb_rating: item.rating,
                        imdb_rank: item.rank,
                        tmdb_id: tmdbDetails.id,
                        title: tmdbDetails.title || tmdbDetails.name,
                        original_title: tmdbDetails.original_title || tmdbDetails.original_name,
                        overview: tmdbDetails.overview,
                        poster_path: tmdbDetails.poster_path,
                        backdrop_path: tmdbDetails.backdrop_path,
                        release_date: tmdbDetails.release_date || tmdbDetails.first_air_date,
                        genres: tmdbDetails.genres.map(g => g.name),
                        vote_average: tmdbDetails.vote_average,
                        vote_count: tmdbDetails.vote_count,
                        media_type: mediaType,
                        origin_country: tmdbDetails.origin_country || [],
                    };
                });

                const settledResults = await Promise.allSettled(promises);
                settledResults.forEach(result => {
                    if (result.status === 'fulfilled' && result.value) {
                        enrichedItems.push(result.value);
                    }
                });
                
                await sleep(500);
            }
            enrichedData[chartKey][regionKey] = enrichedItems;
        }
    }
    
    console.log('Enrichment process finished.');
    return enrichedData;
}

async function main() {
    console.log('Starting main build process...');
    const startTime = Date.now();
    try {
        const rawImdbData = await buildImdbData();
        const finalData = await enrichDataWithTmdb(rawImdbData);
        const output = {
            buildTimestamp: new Date().toISOString(),
            imdb: finalData
        };
        await fs.writeFile('imdb_precomputed_data.json', JSON.stringify(output)); // 使用更紧凑的格式
        const duration = (Date.now() - startTime) / 1000;
        console.log(`\n✅ Build process successful! Took ${duration.toFixed(2)} seconds.`);
        console.log('`imdb_precomputed_data.json` has been generated.');
    } catch (error) {
        console.error('\n❌ FATAL ERROR during build process:', error);
        process.exit(1);
    }
}

main();
