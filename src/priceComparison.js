import logger from './logger.js';

async function verifyPriceColumns(connection) {
  try {
    logger.info('Checking required columns in price tables...');
    // Check for required columns in both tables
    const requiredColumns = {
      old: ['sku_code', 'price_book_id', 'mrp', 'rsp', 'spp'],
      new: ['sku_code', 'price_book_id', 'mrp', 'rsp', 'spp']
    };

    logger.info('Fetching columns from old price table (vendor_item_price_mapping)...');
    const [oldColumns] = await connection.query(`
      SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS 
      WHERE TABLE_NAME = 'vendor_item_price_mapping'
    `);

    logger.info('Fetching columns from new price table (im_sku_price_mapping)...');
    const [newColumns] = await connection.query(`
      SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS 
      WHERE TABLE_NAME = 'im_sku_price_mapping'
    `);

    const oldColumnNames = oldColumns.map(col => col.COLUMN_NAME.toLowerCase());
    const newColumnNames = newColumns.map(col => col.COLUMN_NAME.toLowerCase());

    const missingColumns = {
      oldTable: requiredColumns.old.filter(col => !oldColumnNames.includes(col.toLowerCase())),
      newTable: requiredColumns.new.filter(col => !newColumnNames.includes(col.toLowerCase()))
    };

    if (missingColumns.oldTable.length > 0) {
      logger.warning(`Found ${missingColumns.oldTable.length} missing columns in old price table:`, missingColumns.oldTable);
    } else {
      logger.success('✓ All required columns present in old price table');
    }

    if (missingColumns.newTable.length > 0) {
      logger.warning(`Found ${missingColumns.newTable.length} missing columns in new price table:`, missingColumns.newTable);
    } else {
      logger.success('✓ All required columns present in new price table');
    }

    return missingColumns;
  } catch (error) {
    logger.error('Error verifying price columns:', error);
    throw error;
  }
}

async function comparePrices(connection, limit = null) {
  try {
    logger.info('Starting price comparison between old and new tables...');
    const startTime = Date.now();
    
    const limitClause = limit ? ` LIMIT ${limit}` : '';
    const [comparisonResults] = await connection.query(`
      WITH combined_skus AS (
        SELECT sku_code, price_book_id FROM vendor_item_price_mapping
        UNION
        SELECT sku_code, price_book_id FROM im_sku_price_mapping
      )
      SELECT 
        COALESCE(o.sku_code, n.sku_code) as sku_code,
        COALESCE(o.price_book_id, n.price_book_id) as price_book_id,
        o.mrp as old_mrp, n.mrp as new_mrp,
        o.rsp as old_rsp, n.rsp as new_rsp,
        o.spp as old_spp, n.spp as new_spp,
        CASE 
          WHEN o.sku_code IS NULL THEN 'missing_in_old'
          WHEN n.sku_code IS NULL THEN 'missing_in_new'
          ELSE 'exists_in_both'
        END as status
      FROM combined_skus c
      LEFT JOIN vendor_item_price_mapping o
        ON c.sku_code = o.sku_code 
        AND c.price_book_id = o.price_book_id
      LEFT JOIN im_sku_price_mapping n
        ON c.sku_code = n.sku_code 
        AND c.price_book_id = n.price_book_id
      WHERE o.sku_code IS NULL 
        OR n.sku_code IS NULL
        OR o.mrp != n.mrp 
        OR o.rsp != n.rsp 
        OR o.spp != n.spp
      ${limitClause}
    `);

    const mismatches = [];
    const missing = {
      missingInNew: [],
      missingInOld: []
    };

    for (const row of comparisonResults) {
      if (row.status === 'missing_in_new') {
        missing.missingInNew.push({
          sku_code: row.sku_code,
          price_book_id: row.price_book_id,
          prices: {
            mrp: row.old_mrp,
            rsp: row.old_rsp,
            spp: row.old_spp
          }
        });
        logger.warning(`SKU ${row.sku_code} with price book ${row.price_book_id} missing in new table`);
      } else if (row.status === 'missing_in_old') {
        missing.missingInOld.push({
          sku_code: row.sku_code,
          price_book_id: row.price_book_id,
          prices: {
            mrp: row.new_mrp,
            rsp: row.new_rsp,
            spp: row.new_spp
          }
        });
        logger.warning(`SKU ${row.sku_code} with price book ${row.price_book_id} missing in old table`);
      } else {
        const differences = [];
        [
          { field: 'mrp', old: row.old_mrp, new: row.new_mrp },
          { field: 'rsp', old: row.old_rsp, new: row.new_rsp },
          { field: 'spp', old: row.old_spp, new: row.new_spp }
        ].forEach(({ field, old, new: newValue }) => {
          if (old !== newValue) {
            logger.warning(`Price mismatch for SKU ${row.sku_code} (price book ${row.price_book_id}): ${field.toUpperCase()} changed from ${old || 'null'} to ${newValue || 'null'}`);
            differences.push({
              field,
              old_value: old,
              new_value: newValue
            });
          }
        });
        if (differences.length > 0) {
          mismatches.push({
            sku_code: row.sku_code,
            price_book_id: row.price_book_id,
            differences
          });
        }
      }
    }

    const endTime = Date.now();
    const duration = (endTime - startTime) / 1000;
    logger.info(`Price comparison completed in ${duration} seconds`);
    
    // Log summary
    logger.info('\nPrice comparison summary:');
    logger.info(`Mismatches found: ${mismatches.length}`);
    logger.info(`Prices missing in new table: ${missing.missingInNew.length}`);
    logger.info(`Prices missing in old table: ${missing.missingInOld.length}`);

    if (mismatches.length === 0 && missing.missingInNew.length === 0 && missing.missingInOld.length === 0) {
      logger.success('✓ All prices match between old and new tables');
    }

    return {
      mismatches,
      missing,
      executionTime: endTime - startTime
    };
  } catch (error) {
    logger.error('Error comparing prices:', error);
    throw error;
  }
}

async function comparePricesOriginal(connection, limit = null) {
  try {
    logger.info('Starting original price comparison approach...');
    const startTime = Date.now();
    
    // First, get all unique SKU codes with their price book combinations
    const limitClause = limit ? ` LIMIT ${limit}` : '';
    const [skuPriceBooks] = await connection.query(`
      SELECT DISTINCT o.sku_code, o.price_book_id
      FROM vendor_item_price_mapping o
      UNION
      SELECT DISTINCT n.sku_code, n.price_book_id
      FROM im_sku_price_mapping n
      ${limitClause}
    `);

    const mismatches = [];
    const missing = {
      missingInNew: [],
      missingInOld: []
    };

    for (const { sku_code, price_book_id } of skuPriceBooks) {
      // Get prices from old table
      const [oldPrices] = await connection.query(`
        SELECT sku_code, price_book_id, mrp, rsp, spp
        FROM vendor_item_price_mapping
        WHERE sku_code = ? AND price_book_id = ?
      `, [sku_code, price_book_id]);

      // Get prices from new table
      const [newPrices] = await connection.query(`
        SELECT sku_code, price_book_id, mrp, rsp, spp
        FROM im_sku_price_mapping
        WHERE sku_code = ? AND price_book_id = ?
      `, [sku_code, price_book_id]);

      // Rest of the comparison logic...
      if (oldPrices.length === 0 && newPrices.length > 0) {
        missing.missingInOld.push({
          sku_code,
          price_book_id,
          prices: newPrices[0]
        });
      } else if (newPrices.length === 0 && oldPrices.length > 0) {
        missing.missingInNew.push({
          sku_code,
          price_book_id,
          prices: oldPrices[0]
        });
      } else if (oldPrices.length > 0 && newPrices.length > 0) {
        const differences = [];
        ['mrp', 'rsp', 'spp'].forEach(field => {
          if (oldPrices[0][field] !== newPrices[0][field]) {
            differences.push({
              field,
              old_value: oldPrices[0][field],
              new_value: newPrices[0][field]
            });
          }
        });
        if (differences.length > 0) {
          mismatches.push({ sku_code, price_book_id, differences });
        }
      }
    }

    const endTime = Date.now();
    logger.info(`Original approach took ${(endTime - startTime) / 1000} seconds`);
    
    return {
      mismatches,
      missing,
      executionTime: endTime - startTime
    };
  } catch (error) {
    logger.error('Error in original comparison approach:', error);
    throw error;
  }
}

async function comparePricesOptimized(connection, limit = null) {
  try {
    logger.info('Starting optimized JOIN comparison approach...');
    const startTime = Date.now();
    
    const limitClause = limit ? ` LIMIT ${limit}` : '';
    const [comparisonResults] = await connection.query(`
      WITH combined_skus AS (
        SELECT sku_code, price_book_id FROM vendor_item_price_mapping
        UNION
        SELECT sku_code, price_book_id FROM im_sku_price_mapping
      )
      SELECT 
        COALESCE(o.sku_code, n.sku_code) as sku_code,
        COALESCE(o.price_book_id, n.price_book_id) as price_book_id,
        o.mrp as old_mrp, n.mrp as new_mrp,
        o.rsp as old_rsp, n.rsp as new_rsp,
        o.spp as old_spp, n.spp as new_spp,
        CASE 
          WHEN o.sku_code IS NULL THEN 'missing_in_old'
          WHEN n.sku_code IS NULL THEN 'missing_in_new'
          ELSE 'exists_in_both'
        END as status
      FROM combined_skus c
      LEFT JOIN vendor_item_price_mapping o
        ON c.sku_code = o.sku_code 
        AND c.price_book_id = o.price_book_id
      LEFT JOIN im_sku_price_mapping n
        ON c.sku_code = n.sku_code 
        AND c.price_book_id = n.price_book_id
      WHERE o.sku_code IS NULL 
        OR n.sku_code IS NULL
        OR o.mrp != n.mrp 
        OR o.rsp != n.rsp 
        OR o.spp != n.spp
      ${limitClause}
    `);

    const mismatches = [];
    const missing = {
      missingInNew: [],
      missingInOld: []
    };

    for (const row of comparisonResults) {
      if (row.status === 'missing_in_new') {
        missing.missingInNew.push({
          sku_code: row.sku_code,
          price_book_id: row.price_book_id,
          prices: {
            mrp: row.old_mrp,
            rsp: row.old_rsp,
            spp: row.old_spp
          }
        });
      } else if (row.status === 'missing_in_old') {
        missing.missingInOld.push({
          sku_code: row.sku_code,
          price_book_id: row.price_book_id,
          prices: {
            mrp: row.new_mrp,
            rsp: row.new_rsp,
            spp: row.new_spp
          }
        });
      } else {
        const differences = [];
        [
          { field: 'mrp', old: row.old_mrp, new: row.new_mrp },
          { field: 'rsp', old: row.old_rsp, new: row.new_rsp },
          { field: 'spp', old: row.old_spp, new: row.new_spp }
        ].forEach(({ field, old, new: newValue }) => {
          if (old !== newValue) {
            differences.push({
              field,
              old_value: old,
              new_value: newValue
            });
          }
        });
        if (differences.length > 0) {
          mismatches.push({
            sku_code: row.sku_code,
            price_book_id: row.price_book_id,
            differences
          });
        }
      }
    }

    const endTime = Date.now();
    logger.info(`Optimized approach took ${(endTime - startTime) / 1000} seconds`);
    
    return {
      mismatches,
      missing,
      executionTime: endTime - startTime
    };
  } catch (error) {
    logger.error('Error in optimized comparison approach:', error);
    throw error;
  }
}

async function runBenchmark(connection) {
  logger.info('Starting benchmark of both approaches with sample size of 1000 records...');
  
  // Run both approaches with same sample size
  const sampleSize = 1000;
  
  const originalResults = await comparePricesOriginal(connection, sampleSize);
  const optimizedResults = await comparePricesOptimized(connection, sampleSize);
  
  logger.info('\nBenchmark Results:');
  logger.info(`Original approach execution time: ${originalResults.executionTime}ms`);
  logger.info(`Optimized approach execution time: ${optimizedResults.executionTime}ms`);
  logger.info(`Difference: ${originalResults.executionTime - optimizedResults.executionTime}ms`);
  
  // Return the results for comparison
  return {
    original: originalResults,
    optimized: optimizedResults
  };
}

export { 
  verifyPriceColumns, 
  comparePrices, 
  comparePricesOptimized,
  comparePricesOriginal 
}; 