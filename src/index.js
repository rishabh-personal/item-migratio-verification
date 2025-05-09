import mysql from 'mysql2/promise';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import fs from 'fs/promises';
import logger from './logger.js';
import ReportGenerator from './reportGenerator.js';
import { verifyPriceColumns, comparePrices } from './priceComparison.js';

// Get the directory path of the current module
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configure dotenv to look for .env file in src directory
dotenv.config({ path: join(__dirname, '.env') });

// Database configuration
const dbConfig = {
  host: process.env.DB_HOST || 'localhost',
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  port: parseInt(process.env.DB_PORT || '3306'),
};

// Get list of tenant databases
const tenantDbs = (process.env.TENANT_DBS || '').split(',').filter(Boolean);

// Add configuration for ignored columns
const IGNORED_COLUMNS = {
  oldTable: ['brand_name', 'department_name', 'id','vendor_sku_detail_id','sku']
};

async function verifySkuCodeMatches(connection) {
  try {
    // Check for SKUs in old table but missing in new table
    const [missingInNew] = await connection.query(`
      SELECT o.sku_code, o.name
      FROM vendor_sku_flat_table o
      LEFT JOIN im_sku_flat_table n ON o.sku_code = n.sku_code
      WHERE n.sku_code IS NULL
    `);

    // Check for SKUs in new table but missing in old table
    const [missingInOld] = await connection.query(`
      SELECT n.sku_code, n.name
      FROM im_sku_flat_table n
      LEFT JOIN vendor_sku_flat_table o ON n.sku_code = o.sku_code
      WHERE o.sku_code IS NULL
    `);

    return { missingInNew, missingInOld };
  } catch (error) {
    logger.error('Error verifying SKU code matches:', error);
    throw error;
  }
}

async function loadAttributeMappings() {
  try {
    const mappingContent = await fs.readFile(join(__dirname, '../schemas/attribute-mappings'), 'utf8');
    const mappings = new Map();
    
    mappingContent.split('\n').forEach(line => {
      if (line.trim()) {
        const [newCol, oldCol] = line.split('->').map(s => s.trim());
        mappings.set(newCol, oldCol);
      }
    });
    
    return mappings;
  } catch (error) {
    logger.error('Error loading attribute mappings:', error);
    throw error;
  }
}

async function loadCategoryMappings() {
  try {
    const mappingContent = await fs.readFile(join(__dirname, '../schemas/category-mappings'), 'utf8');
    const mappings = new Map();
    
    mappingContent.split('\n').forEach(line => {
      if (line.trim()) {
        const [newCol, oldCol] = line.split('->').map(s => s.trim());
        mappings.set(newCol, oldCol);
      }
    });
    
    return mappings;
  } catch (error) {
    logger.error('Error loading category mappings:', error);
    throw error;
  }
}

async function compareAttributeValues(connection, attributeMappings) {
  const mismatches = [];
  logger.info(`Comparing ${attributeMappings.size} attribute mappings...`);
  
  for (const [newCol, oldCol] of attributeMappings) {
    try {
      // Skip if either column name is undefined
      if (!newCol || !oldCol) {
        logger.warning(`Skipping invalid mapping: ${newCol || 'undefined'} -> ${oldCol || 'undefined'}`);
        continue;
      }

      logger.info(`Comparing attribute mapping: ${oldCol} -> ${newCol}`);
      const [results] = await connection.query(`
        SELECT 
          o.sku_code,
          o.${oldCol} as old_value,
          n.${newCol} as new_value
        FROM vendor_sku_flat_table o
        LEFT JOIN im_sku_flat_table n ON o.sku_code = n.sku_code
        WHERE (o.${oldCol} != n.${newCol}
          OR (o.${oldCol} IS NULL AND n.${newCol} IS NOT NULL)
          OR (o.${oldCol} IS NOT NULL AND n.${newCol} IS NULL))
        AND o.sku_code IS NOT NULL
        AND n.sku_code IS NOT NULL
      `);
      
      if (results.length > 0) {
        logger.info(`Found ${results.length} mismatches for ${oldCol} -> ${newCol}`);
        mismatches.push({
          old_column: oldCol,
          new_column: newCol,
          differences: results
        });
      }
    } catch (error) {
      logger.warning(`Error comparing ${oldCol || 'undefined'} -> ${newCol || 'undefined'}: ${error.message}`);
    }
  }
  
  return mismatches;
}

async function compareCategoryValues(connection, categoryMappings) {
  const mismatches = [];
  logger.info(`Comparing ${categoryMappings.size} category mappings...`);
  
  for (const [newCol, oldCol] of categoryMappings) {
    try {
      // Skip if either column name is undefined
      if (!newCol || !oldCol) {
        logger.warning(`Skipping invalid category mapping: ${newCol || 'undefined'} -> ${oldCol || 'undefined'}`);
        continue;
      }

      logger.info(`Comparing category mapping: ${oldCol} -> ${newCol}`);
      const [results] = await connection.query(`
        SELECT 
          o.sku_code,
          o.${oldCol} as old_value,
          n.${newCol} as new_value
        FROM vendor_sku_flat_table o
        LEFT JOIN im_sku_flat_table n ON o.sku_code = n.sku_code
        WHERE (o.${oldCol} != n.${newCol}
          OR (o.${oldCol} IS NULL AND n.${newCol} IS NOT NULL)
          OR (o.${oldCol} IS NOT NULL AND n.${newCol} IS NULL))
        AND o.sku_code IS NOT NULL
        AND n.sku_code IS NOT NULL
      `);
      
      if (results.length > 0) {
        logger.info(`Found ${results.length} mismatches for category ${oldCol} -> ${newCol}`);
        mismatches.push({
          old_column: oldCol,
          new_column: newCol,
          differences: results,
          type: 'category'
        });
      }
    } catch (error) {
      logger.warning(`Error comparing category ${oldCol || 'undefined'} -> ${newCol || 'undefined'}: ${error.message}`);
    }
  }
  
  return mismatches;
}

async function compareCommonColumns(connection, availableColumns) {
  const mismatches = [];
  logger.info(`Comparing ${availableColumns.length} common columns...`);
  
  for (const column of availableColumns) {
    try {
      // Skip if column name is undefined
      if (!column) {
        logger.warning(`Skipping undefined common column`);
        continue;
      }

      logger.info(`Comparing common column: ${column}`);
      const query = `
        SELECT 
          o.sku_code,
          o.${column} as old_value,
          n.${column} as new_value
        FROM vendor_sku_flat_table o
        LEFT JOIN im_sku_flat_table n ON o.sku_code = n.sku_code
        WHERE (o.${column} != n.${column}
          OR (o.${column} IS NULL AND n.${column} IS NOT NULL)
          OR (o.${column} IS NOT NULL AND n.${column} IS NULL))
        AND o.sku_code IS NOT NULL
        AND n.sku_code IS NOT NULL
      `;

      const [results] = await connection.query(query);
      
      if (results.length > 0) {
        logger.info(`Found ${results.length} mismatches for column ${column}`);
        mismatches.push({
          column,
          differences: results
        });
      }
    } catch (error) {
      logger.warning(`Error comparing common column ${column || 'undefined'}: ${error.message}`);
    }
  }
  
  return mismatches;
}

async function getTableColumns(connection, tableName) {
  try {
    const [columns] = await connection.query(`
      SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY
      FROM INFORMATION_SCHEMA.COLUMNS 
      WHERE TABLE_NAME = ?
      ORDER BY ORDINAL_POSITION
    `, [tableName]);
    
    return columns.map(col => col.COLUMN_NAME.toLowerCase());
  } catch (error) {
    logger.error(`Error getting columns for table ${tableName}:`, error);
    throw error;
  }
}

async function verifyAttributeColumns(connection, attributeMappings) {
  try {
    logger.info('Getting columns from both flat tables...');
    // Get columns from both flat tables
    const oldTableColumns = await getTableColumns(connection, 'vendor_sku_flat_table');
    const newTableColumns = await getTableColumns(connection, 'im_sku_flat_table');
    logger.info(`Found ${oldTableColumns.length} columns in old table and ${newTableColumns.length} columns in new table`);
    
    const validMappings = new Map();
    const missingColumns = {
      oldTable: [],
      newTable: []
    };
    
    // Check each mapping
    logger.info('Verifying attribute mappings...');
    for (const [newCol, oldCol] of attributeMappings) {
      // Skip ignored columns
      if (IGNORED_COLUMNS.oldTable.includes(oldCol)) {
        logger.info(`Skipping ignored column: ${oldCol}`);
        continue;
      }

      const isOldColPresent = oldTableColumns.includes(oldCol.toLowerCase());
      const isNewColPresent = newTableColumns.includes(newCol.toLowerCase());
      
      if (isOldColPresent && isNewColPresent) {
        validMappings.set(newCol, oldCol);
      } else {
        if (!isOldColPresent) {
          logger.warning(`Column ${oldCol} missing in old table`);
          missingColumns.oldTable.push({ mapping: `${newCol} -> ${oldCol}`, missingColumn: oldCol });
        }
        if (!isNewColPresent) {
          logger.warning(`Column ${newCol} missing in new table`);
          missingColumns.newTable.push({ mapping: `${newCol} -> ${oldCol}`, missingColumn: newCol });
        }
      }
    }
    
    return { validMappings, missingColumns };
  } catch (error) {
    logger.error('Error verifying attribute columns:', error);
    throw error;
  }
}

async function verifyCommonColumns(connection) {
  const desiredCommonColumns = [
    'name',
    'short_description',
    'long_description',
    'purchase_uom_name',
    'selling_uom_name',
    'uom_factor',
    'tax_type',
    'is_active',
    'ref_item_code',
    'sku_code',
    'ref_sku_code',
    'sku',
    'purchase_uom_id',
    'purchase_uom_type',
    'selling_uom_id',
    'selling_uom_name',
    'selling_uom_type'
  ];
  
  try {
    logger.info('Getting columns from both flat tables...');
    const oldTableColumns = await getTableColumns(connection, 'vendor_sku_flat_table');
    const newTableColumns = await getTableColumns(connection, 'im_sku_flat_table');
    logger.info(`Found ${oldTableColumns.length} columns in old table and ${newTableColumns.length} columns in new table`);
    
    const availableColumns = [];
    const missingColumns = {
      oldTable: [],
      newTable: []
    };
    
    logger.info('Checking availability of common columns...');
    desiredCommonColumns.forEach(column => {
      // Skip ignored columns
      if (IGNORED_COLUMNS.oldTable.includes(column)) {
        logger.info(`Skipping ignored common column: ${column}`);
        return;
      }

      const isOldColPresent = oldTableColumns.includes(column.toLowerCase());
      const isNewColPresent = newTableColumns.includes(column.toLowerCase());
      
      if (isOldColPresent && isNewColPresent) {
        logger.info(`Found common column: ${column}`);
        availableColumns.push(column);
      } else {
        if (!isOldColPresent) {
          logger.warning(`Column ${column} missing in old table`);
          missingColumns.oldTable.push(column);
        }
        if (!isNewColPresent) {
          logger.warning(`Column ${column} missing in new table`);
          missingColumns.newTable.push(column);
        }
      }
    });
    
    return { availableColumns, missingColumns };
  } catch (error) {
    logger.error('Error verifying common columns:', error);
    throw error;
  }
}

async function verifyDatabase(dbName, attributeMappings, categoryMappings) {
  logger.info(`\nVerifying database: ${dbName}`);
  logger.info('Step 1: Connecting to database...');
  
  const connection = await mysql.createConnection({
    ...dbConfig,
    database: dbName
  });
  
  const results = {
    missingAttributeColumns: null,
    missingCategoryColumns: null,
    missingCommonColumns: null,
    missingPriceColumns: null,
    skuMismatches: null,
    attributeMismatches: null,
    categoryMismatches: null,
    commonColumnMismatches: null,
    priceMismatches: null
  };
  
  try {
    // First verify available columns
    logger.info('Step 2: Verifying attribute columns...');
    const { validMappings, missingColumns: missingAttributeColumns } = await verifyAttributeColumns(connection, attributeMappings);
    logger.info(`Found ${validMappings.size} valid attribute mappings`);

    logger.info('Step 3: Verifying category columns...');
    const { validMappings: validCategoryMappings, missingColumns: missingCategoryColumns } = await verifyAttributeColumns(connection, categoryMappings);
    logger.info(`Found ${validCategoryMappings.size} valid category mappings`);

    logger.info('Step 4: Verifying common columns...');
    const { availableColumns, missingColumns: missingCommonColumns } = await verifyCommonColumns(connection);
    logger.info(`Found ${availableColumns.length} common columns to compare`);
    
    results.missingAttributeColumns = missingAttributeColumns;
    results.missingCategoryColumns = missingCategoryColumns;
    results.missingCommonColumns = missingCommonColumns;
    
    // Log missing columns
    if (missingAttributeColumns.oldTable.length > 0 || missingAttributeColumns.newTable.length > 0) {
      logger.warning('\nMissing attribute columns detected:');
      
      if (missingAttributeColumns.oldTable.length > 0) {
        logger.warning('\nColumns missing in old table (vendor_sku_flat_table):');
        logger.table(missingAttributeColumns.oldTable);
      }
      
      if (missingAttributeColumns.newTable.length > 0) {
        logger.warning('\nColumns missing in new table (im_sku_flat_table):');
        logger.table(missingAttributeColumns.newTable);
      }
    }

    if (missingCategoryColumns.oldTable.length > 0 || missingCategoryColumns.newTable.length > 0) {
      logger.warning('\nMissing category columns detected:');
      
      if (missingCategoryColumns.oldTable.length > 0) {
        logger.warning('\nCategory columns missing in old table (vendor_sku_flat_table):');
        logger.table(missingCategoryColumns.oldTable);
      }
      
      if (missingCategoryColumns.newTable.length > 0) {
        logger.warning('\nCategory columns missing in new table (im_sku_flat_table):');
        logger.table(missingCategoryColumns.newTable);
      }
    }
    
    if (missingCommonColumns.oldTable.length > 0 || missingCommonColumns.newTable.length > 0) {
      logger.warning('\nMissing common columns detected:');
      
      if (missingCommonColumns.oldTable.length > 0) {
        logger.warning('\nCommon columns missing in old table:');
        logger.table(missingCommonColumns.oldTable.map(col => ({ column: col })));
      }
      
      if (missingCommonColumns.newTable.length > 0) {
        logger.warning('\nCommon columns missing in new table:');
        logger.table(missingCommonColumns.newTable.map(col => ({ column: col })));
      }
    }
    
    // Verify SKU code matches
    logger.info('Step 5: Verifying SKU code matches...');
    const skuMismatches = await verifySkuCodeMatches(connection);
    results.skuMismatches = skuMismatches;
    logger.info(`Found ${skuMismatches.missingInNew.length} SKUs missing in new table and ${skuMismatches.missingInOld.length} SKUs missing in old table`);
    
    // Report SKU mismatches
    if (skuMismatches.missingInNew.length > 0 || skuMismatches.missingInOld.length > 0) {
      logger.error('\nSKU code mismatches found:');
      
      if (skuMismatches.missingInNew.length > 0) {
        logger.warning('\nSKUs present in old table but missing in new table:');
        logger.table(skuMismatches.missingInNew);
      }
      
      if (skuMismatches.missingInOld.length > 0) {
        logger.warning('\nSKUs present in new table but missing in old table:');
        logger.table(skuMismatches.missingInOld);
      }
    } else {
      logger.success('✓ All SKU codes match between old and new tables');
    }
    
    // Compare attribute values using only valid mappings
    logger.info('Step 6: Comparing attribute values...');
    const attributeMismatches = await compareAttributeValues(connection, validMappings);
    results.attributeMismatches = attributeMismatches;
    logger.info(`Found ${attributeMismatches.length} attribute mismatches`);

    // Compare category values
    logger.info('Step 7: Comparing category values...');
    const categoryMismatches = await compareCategoryValues(connection, validCategoryMappings);
    results.categoryMismatches = categoryMismatches;
    logger.info(`Found ${categoryMismatches.length} category mismatches`);
    
    // Compare only available common columns
    logger.info('Step 8: Comparing common columns...');
    const commonColumnMismatches = await compareCommonColumns(connection, availableColumns);
    results.commonColumnMismatches = commonColumnMismatches;
    logger.info(`Found ${commonColumnMismatches.length} common column mismatches`);
    
    // Report value mismatches
    if (attributeMismatches.length === 0 && categoryMismatches.length === 0 && commonColumnMismatches.length === 0) {
      logger.success('✓ All values match between old and new tables for available columns');
    } else {
      logger.error('\nValue mismatches found:');
      
      // Combine all mismatches for logging
      const allMismatches = [
        ...attributeMismatches.map(m => ({ ...m, type: 'attribute' })),
        ...categoryMismatches.map(m => ({ ...m, type: 'category' })),
        ...commonColumnMismatches.map(m => ({ ...m, type: 'common' }))
      ];
      logger.info(`Processing ${allMismatches.length} total mismatches for display`);

      // Group mismatches by SKU code for cleaner logging
      const skuMap = new Map();
      
      allMismatches.forEach(mismatch => {
        if (!mismatch.differences) return;
        
        mismatch.differences.forEach(diff => {
          const skuCode = diff.sku_code || 'N/A';
          if (!skuMap.has(skuCode)) {
            skuMap.set(skuCode, []);
          }
          
          const columnName = mismatch.type === 'attribute' || mismatch.type === 'category' ? 
            `${mismatch.old_column} -> ${mismatch.new_column}` : 
            mismatch.column;
            
          skuMap.get(skuCode).push({
            column_name: columnName,
            column_type: mismatch.type.charAt(0).toUpperCase() + mismatch.type.slice(1),
            old_value: diff.old_value || 'null',
            new_value: diff.new_value || 'null'
          });
        });
      });

      logger.info(`Grouped mismatches by ${skuMap.size} unique SKU codes`);

      // Log mismatches grouped by SKU code
      for (const [skuCode, differences] of skuMap) {
        logger.info(`\nSKU Code: ${skuCode} (${differences.length} differences)`);
        logger.table(differences);
      }
    }

    // Add price verification steps
    logger.info('Step 9: Verifying price columns...');
    const missingPriceColumns = await verifyPriceColumns(connection);
    results.missingPriceColumns = missingPriceColumns;
    logger.info(`Found ${missingPriceColumns.oldTable.length} missing columns in old price table and ${missingPriceColumns.newTable.length} in new price table`);

    if (missingPriceColumns.oldTable.length === 0 && missingPriceColumns.newTable.length === 0) {
      logger.info('Step 10: Comparing prices...');
      const priceComparison = await comparePrices(connection);
      results.priceMismatches = priceComparison;
      logger.info(`Found ${priceComparison.mismatches.length} price mismatches`);
      logger.info(`Found ${priceComparison.missing.missingInNew.length} prices missing in new table`);
      logger.info(`Found ${priceComparison.missing.missingInOld.length} prices missing in old table`);
    } else {
      logger.warning('Skipping price comparison due to missing required columns');
    }

  } catch (error) {
    logger.error(`Error verifying database ${dbName}:`, error);
  } finally {
    logger.info('Step 11: Closing database connection...');
    await connection.end();
  }
  
  return results;
}

async function main() {
  try {
    if (!process.env.DB_USER || !process.env.DB_PASSWORD) {
      logger.error('Error: Database credentials not provided. Please check your .env file.');
      process.exit(1);
    }
    
    if (tenantDbs.length === 0) {
      logger.error('Error: No tenant databases specified. Please set TENANT_DBS in your .env file.');
      process.exit(1);
    }
    
    // Initialize report generator
    const reportGenerator = new ReportGenerator();
    await reportGenerator.initialize();
    
    // Load attribute mappings
    const attributeMappings = await loadAttributeMappings();
    
    // Load category mappings
    const categoryMappings = await loadCategoryMappings();
    
    logger.info('Starting verification process...');
    logger.info(`Loaded ${attributeMappings.size} attribute mappings`);
    logger.info(`Loaded ${categoryMappings.size} category mappings`);
    
    // Verify each database and generate reports
    for (const dbName of tenantDbs) {
      const results = await verifyDatabase(dbName.trim(), attributeMappings, categoryMappings);
      await reportGenerator.generateDatabaseReport(dbName.trim(), results);
    }
    
    // Generate index page
    const indexPath = await reportGenerator.generateIndexPage();
    
    logger.info('\nVerification process completed.');
    logger.info(`Reports generated in: ${dirname(indexPath)}`);
    logger.info(`Open ${indexPath} to view the summary.`);
  } catch (error) {
    logger.error('Fatal error:', error);
    process.exit(1);
  } finally {
    logger.close();
  }
}

main().catch(error => {
  logger.error('Fatal error:', error);
  logger.close();
  process.exit(1);
}); 