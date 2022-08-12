const { PromisePool } = require('@supercharge/promise-pool')
const { 
    getDBPool, 
    getLastProcessedSlotNum, 
    getUnprocessedRawData,
    getCountOfUnprocessedRawData,
    processRow,
    pushProcessorMetricsToCloudwatch
} = require('./utils');

/**
 * Process incoming account updates
 * 
 * 1. Get unprocessed updates from postgres
 * 2. For each update, fetch idl, parse and write to the right decoded tables
 * 
 */
async function main() {
    let metricData = [];

    const poolProcessedId = getDBPool(1);
    while(true){
        try {
            const client = await poolProcessedId.connect();
        
            const lastProcessedSlotNum = await getLastProcessedSlotNum(client);
            console.log("lastProcessedSlotNum: ", lastProcessedSlotNum);
        
            const rows = await getUnprocessedRawData(client, lastProcessedSlotNum);
            const cntr = await getCountOfUnprocessedRawData(client, lastProcessedSlotNum);

            await client.release();

            // don't open more than 250 connections!
            const concurrencyCount = Math.min(Math.max(rows.length, 1), 250);
            const pool = getDBPool(concurrencyCount);

            const errors = [];
            const { results } = await PromisePool
                .withConcurrency(concurrencyCount)
                .for(rows)
                .handleError(async (error, row) => {
                    errors.push(error);
                    // try to process failed row max 2 times
                    console.log('Error processing row: ', row, error); 
                    try { 
                        await processRow(pool, row);
                    } catch (e) {
                        try {
                            await processRow(pool, row); 
                        } catch (e) {
                            // TODO: Push this error as a permananent error that needs manual intervention
                            console.log('Exhausted  2 re-tries, still failed', e);
                        }
                    }
                })
                .process(async (row, index, promisePool) => {
                    await processRow(pool, row);
                });
            console.log('processed all updates, Succesfully processed: ', results.length, ' Failed to process:  ', errors.length);

            await pool.end();

            // create metrics and push to Cloudwatch
            await pushProcessorMetricsToCloudwatch(metricData, results.length - errors.length, errors.length, cntr);
            
            await new Promise(r => setTimeout(r, 1000));
        } catch (error) {
            console.log('Error in main process', error);
        }
    }
}

main();