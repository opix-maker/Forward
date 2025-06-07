import fs from 'fs/promises';
import { buildImdbData } from './src/builders/imdb_builder.js';
import { findByImdbId } from './src/utils/tmdb_api.js';
import { sleep } from './src/utils/helpers.js';

const MAX_CONCURRENT_ENRICHMENTS = 15;

async function generateTmdbInstructions(rawImdbData) {
    console.log('Generating TMDB instruction list from raw IMDb data...');
    const instructionData = {};

    for (const [chartKey, regions] of Object.entries(rawImdbData)) {
        instructionData[chartKey] = {};
        console.log(`- Processing chart type: ${chartKey}`);

        for (const [regionKey, items] of Object.entries(regions)) {
            console.log(`  - Processing region: ${regionKey} (${items.length} items)`);
            const instructions = [];
            
            for (let i = 0; i < items.length; i += MAX_CONCURRENT_ENRICHMENTS) {
                const batch = items.slice(i, i + MAX_CONCURRENT_ENRICHMENTS);
                const promises = batch.map(async (item) => {
                    const tmdbSearchResult = await findByImdbId(item.imdbId);
                    if (!tmdbSearchResult || !tmdbSearchResult.id) {
                        console.warn(`  - TMDB ID not found for IMDb ID: ${item.imdbId} (${item.title})`);
                        return null;
                    }

                    const mediaType = tmdbSearchResult.media_type || 'movie';
                    
                    // 只生成App需要的、最核心的“指令”对象
                    return {
                        id: `${mediaType}.${tmdbSearchResult.id}`, // 关键：生成 "movie.123" 格式
                        type: "tmdb",
                        title: tmdbSearchResult.title || tmdbSearchResult.name || item.title, // 保留标题用于调试
                        // 不再需要其他任何TMDB详情字段
                    };
                });

                const settledResults = await Promise.allSettled(promises);
                settledResults.forEach(result => {
                    if (result.status === 'fulfilled' && result.value) {
                        instructions.push(result.value);
                    }
                });
                
                await sleep(250); // 在批次之间短暂等待
            }
            instructionData[chartKey][regionKey] = instructions;
        }
    }
    
    console.log('Instruction list generation finished.');
    return instructionData;
}

async function main() {
    console.log('Starting main build process...');
    const startTime = Date.now();
    try {
        const rawImdbData = await buildImdbData();
        const finalInstructions = await generateTmdbInstructions(rawImdbData);
        const output = {
            buildTimestamp: new Date().toISOString(),
            imdb: finalInstructions
        };
        await fs.writeFile('imdb_precomputed_data.json', JSON.stringify(output));
        const duration = (Date.now() - startTime) / 1000;
        console.log(`\n✅ Build process successful! Took ${duration.toFixed(2)} seconds.`);
        console.log('`imdb_precomputed_data.json` has been generated.');
    } catch (error) {
        console.error('\n❌ FATAL ERROR during build process:', error);
        process.exit(1);
    }
}

main();
