import logger from './logger.js';

async function verifyPriceColumns(connection) {
  try {
    // Check for required columns in both tables
    const requiredColumns = {
      old: ['sku_code', 'price_book_id', 'mrp', 'rsp', 'spp'],
      new: ['sku_code', 'price_book_id', 'mrp', 'rsp', 'spp']
    };

    const [oldColumns] = await connection.query(`
      SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS 
      WHERE TABLE_NAME = 'vendor_item_price_mapping'
    `);

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

    return missingColumns;
  } catch (error) {
    logger.error('Error verifying price columns:', error);
    throw error;
  }
}

async function comparePrices(connection) {
  try {
    // First, get all unique SKU codes with their price book combinations
    const [skuPriceBooks] = await connection.query(`
      SELECT DISTINCT o.sku_code, o.price_book_id
      FROM vendor_item_price_mapping o
      UNION
      SELECT DISTINCT n.sku_code, n.price_book_id
      FROM im_sku_price_mapping n
    `);

    const mismatches = [];
    const missing = {
      missingInNew: [],
      missingInOld: []
    };

    // Compare prices for each SKU and price book combination
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

      // Check if prices exist in both tables
      if (oldPrices.length === 0 && newPrices.length > 0) {
        missing.missingInOld.push({
          sku_code,
          price_book_id,
          prices: newPrices[0]
        });
        continue;
      }

      if (newPrices.length === 0 && oldPrices.length > 0) {
        missing.missingInNew.push({
          sku_code,
          price_book_id,
          prices: oldPrices[0]
        });
        continue;
      }

      // Compare prices if they exist in both tables
      if (oldPrices.length > 0 && newPrices.length > 0) {
        const priceFields = ['mrp', 'rsp', 'spp'];
        const differences = [];

        for (const field of priceFields) {
          const oldValue = oldPrices[0][field];
          const newValue = newPrices[0][field];

          if (oldValue !== newValue) {
            differences.push({
              field,
              old_value: oldValue,
              new_value: newValue
            });
          }
        }

        if (differences.length > 0) {
          mismatches.push({
            sku_code,
            price_book_id,
            differences
          });
        }
      }
    }

    return {
      mismatches,
      missing
    };
  } catch (error) {
    logger.error('Error comparing prices:', error);
    throw error;
  }
}

export { verifyPriceColumns, comparePrices }; 