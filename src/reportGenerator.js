import fs from 'fs/promises';
import { createWriteStream } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

class ReportGenerator {
  constructor() {
    this.reportsDir = join(__dirname, '../reports');
    this.timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    this.databaseResults = new Map();
  }

  async initialize() {
    try {
      await fs.mkdir(this.reportsDir, { recursive: true });
      await fs.mkdir(join(this.reportsDir, this.timestamp), { recursive: true });
    } catch (error) {
      console.error('Error creating reports directory:', error);
      throw error;
    }
  }

  generateMismatchSummary(dbName, results) {
    const summary = {
      dbName,
      missingAttributeColumns: {
        oldTable: results.missingAttributeColumns?.oldTable?.length || 0,
        newTable: results.missingAttributeColumns?.newTable?.length || 0
      },
      missingCategoryColumns: {
        oldTable: results.missingCategoryColumns?.oldTable?.length || 0,
        newTable: results.missingCategoryColumns?.newTable?.length || 0
      },
      missingCommonColumns: {
        oldTable: results.missingCommonColumns?.oldTable?.length || 0,
        newTable: results.missingCommonColumns?.newTable?.length || 0
      },
      skuMismatches: {
        missingInNew: results.skuMismatches?.missingInNew?.length || 0,
        missingInOld: results.skuMismatches?.missingInOld?.length || 0
      },
      valueMismatches: {
        attributes: results.attributeMismatches?.length || 0,
        categories: results.categoryMismatches?.length || 0,
        commonColumns: results.commonColumnMismatches?.length || 0
      }
    };

    this.databaseResults.set(dbName, summary);
    return summary;
  }

  generateTableHTML(data, headers = null) {
    if (!data || data.length === 0) return '<p>No data available</p>';

    const tableHeaders = headers || Object.keys(data[0]);
    
    return `
      <table class="table">
        <thead>
          <tr>
            ${tableHeaders.map(header => `<th>${header}</th>`).join('')}
          </tr>
        </thead>
        <tbody>
          ${data.map(row => `
            <tr>
              ${tableHeaders.map(header => `<td>${row[header] ?? ''}</td>`).join('')}
            </tr>
          `).join('')}
        </tbody>
      </table>
    `;
  }

  generateMismatchTableHTML(mismatches) {
    if (!mismatches || mismatches.length === 0) return '<p>No mismatches found</p>';

    // Create a map to group differences by SKU code
    const skuMap = new Map();

    mismatches.forEach(mismatch => {
      const type = mismatch.type || 'attribute';
      if (!mismatch.differences) return;

      mismatch.differences.forEach(diff => {
        const columnName = type === 'attribute' || type === 'category' ? 
          `${mismatch.old_column} -> ${mismatch.new_column}` : 
          mismatch.column;

        const columnType = type.charAt(0).toUpperCase() + type.slice(1);
        const skuCode = diff.sku_code || 'N/A';

        if (!skuMap.has(skuCode)) {
          skuMap.set(skuCode, []);
        }

        skuMap.get(skuCode).push({
          column_name: columnName || 'N/A',
          column_type: columnType,
          old_value: diff.old_value || '<em>null</em>',
          new_value: diff.new_value || '<em>null</em>'
        });
      });
    });

    if (skuMap.size === 0) return '<p>No valid mismatches found</p>';

    return `
      <table class="table table-striped table-bordered">
        <thead>
          <tr>
            <th>SKU Code</th>
            <th>Column Name</th>
            <th>Column Type</th>
            <th>Old Value</th>
            <th>New Value</th>
          </tr>
        </thead>
        <tbody>
          ${Array.from(skuMap.entries()).map(([skuCode, differences]) => 
            differences.map((diff, index) => `
              ${index === 0 ? `<tr>
                <td rowspan="${differences.length}">${skuCode}</td>` : '<tr>'}
                <td>${diff.column_name}</td>
                <td><span class="badge ${
                  diff.column_type === 'Attribute' ? 'bg-primary' : 
                  diff.column_type === 'Category' ? 'bg-warning' : 
                  'bg-secondary'
                }">${diff.column_type}</span></td>
                <td class="text-danger">${diff.old_value}</td>
                <td class="text-success">${diff.new_value}</td>
              </tr>`
            ).join('')
          ).join('')}
        </tbody>
      </table>
    `;
  }

  async generateDatabaseReport(dbName, results) {
    const summary = this.generateMismatchSummary(dbName, results);
    const reportPath = join(this.reportsDir, this.timestamp, `${dbName}.html`);

    const html = `
      <!DOCTYPE html>
      <html>
      <head>
        <title>Verification Report - ${dbName}</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
          body { padding: 20px; }
          .section { margin-bottom: 30px; }
          .table { margin-top: 10px; }
          .summary-card { margin-bottom: 20px; }
          .alert { margin-top: 10px; }
          .badge { font-size: 0.9em; }
          td.text-danger { color: #dc3545 !important; }
          td.text-success { color: #198754 !important; }
          em { color: #6c757d; }
        </style>
      </head>
      <body>
        <div class="container">
          <h1 class="mb-4">Verification Report - ${dbName}</h1>
          <p class="text-muted">Generated on: ${new Date().toLocaleString()}</p>

          <div class="section">
            <h2>Summary</h2>
            <div class="row">
              <div class="col-md-6">
                <div class="card summary-card">
                  <div class="card-body">
                    <h5 class="card-title">Missing Columns</h5>
                    <ul class="list-group list-group-flush">
                      <li class="list-group-item">Old Table: 
                        ${summary.missingAttributeColumns.oldTable} attribute(s), 
                        ${summary.missingCategoryColumns.oldTable} category(ies), 
                        ${summary.missingCommonColumns.oldTable} common column(s)
                      </li>
                      <li class="list-group-item">New Table: 
                        ${summary.missingAttributeColumns.newTable} attribute(s), 
                        ${summary.missingCategoryColumns.newTable} category(ies), 
                        ${summary.missingCommonColumns.newTable} common column(s)
                      </li>
                    </ul>
                  </div>
                </div>
              </div>
              <div class="col-md-6">
                <div class="card summary-card">
                  <div class="card-body">
                    <h5 class="card-title">Mismatches</h5>
                    <ul class="list-group list-group-flush">
                      <li class="list-group-item">SKUs missing in new table: ${summary.skuMismatches.missingInNew}</li>
                      <li class="list-group-item">SKUs missing in old table: ${summary.skuMismatches.missingInOld}</li>
                      <li class="list-group-item">Total value mismatches: ${
                        summary.valueMismatches.attributes + 
                        summary.valueMismatches.categories + 
                        summary.valueMismatches.commonColumns
                      }</li>
                    </ul>
                  </div>
                </div>
              </div>
            </div>
          </div>

          ${results.missingAttributeColumns?.oldTable?.length || results.missingAttributeColumns?.newTable?.length ? `
            <div class="section">
              <h2>Missing Attribute Columns</h2>
              ${results.missingAttributeColumns.oldTable?.length ? `
                <h4>Old Table (vendor_sku_flat_table)</h4>
                ${this.generateTableHTML(results.missingAttributeColumns.oldTable)}
              ` : ''}
              ${results.missingAttributeColumns.newTable?.length ? `
                <h4>New Table (im_sku_flat_table)</h4>
                ${this.generateTableHTML(results.missingAttributeColumns.newTable)}
              ` : ''}
            </div>
          ` : ''}

          ${results.missingCategoryColumns?.oldTable?.length || results.missingCategoryColumns?.newTable?.length ? `
            <div class="section">
              <h2>Missing Category Columns</h2>
              ${results.missingCategoryColumns.oldTable?.length ? `
                <h4>Old Table (vendor_sku_flat_table)</h4>
                ${this.generateTableHTML(results.missingCategoryColumns.oldTable)}
              ` : ''}
              ${results.missingCategoryColumns.newTable?.length ? `
                <h4>New Table (im_sku_flat_table)</h4>
                ${this.generateTableHTML(results.missingCategoryColumns.newTable)}
              ` : ''}
            </div>
          ` : ''}

          ${results.missingCommonColumns?.oldTable?.length || results.missingCommonColumns?.newTable?.length ? `
            <div class="section">
              <h2>Missing Common Columns</h2>
              ${results.missingCommonColumns.oldTable?.length ? `
                <h4>Old Table</h4>
                ${this.generateTableHTML(results.missingCommonColumns.oldTable.map(col => ({ column: col })))}
              ` : ''}
              ${results.missingCommonColumns.newTable?.length ? `
                <h4>New Table</h4>
                ${this.generateTableHTML(results.missingCommonColumns.newTable.map(col => ({ column: col })))}
              ` : ''}
            </div>
          ` : ''}

          ${results.skuMismatches?.missingInNew?.length || results.skuMismatches?.missingInOld?.length ? `
            <div class="section">
              <h2>SKU Mismatches</h2>
              ${results.skuMismatches.missingInNew?.length ? `
                <h4>SKUs present in old table but missing in new table</h4>
                ${this.generateTableHTML(results.skuMismatches.missingInNew)}
              ` : ''}
              ${results.skuMismatches.missingInOld?.length ? `
                <h4>SKUs present in new table but missing in old table</h4>
                ${this.generateTableHTML(results.skuMismatches.missingInOld)}
              ` : ''}
            </div>
          ` : ''}

          ${(results.attributeMismatches?.length || results.categoryMismatches?.length || results.commonColumnMismatches?.length) ? `
            <div class="section">
              <h2>Value Mismatches</h2>
              <div class="alert alert-info">
                <strong>Note:</strong> Values are color-coded - <span class="text-danger">red for old values</span> and <span class="text-success">green for new values</span>. <em>null</em> indicates missing values.
              </div>
              ${this.generateMismatchTableHTML([
                ...(results.attributeMismatches?.map(m => ({ ...m, type: 'attribute' })) || []),
                ...(results.categoryMismatches?.map(m => ({ ...m, type: 'category' })) || []),
                ...(results.commonColumnMismatches?.map(m => ({ ...m, type: 'common' })) || [])
              ])}
            </div>
          ` : ''}
        </div>
      </body>
      </html>
    `;

    await fs.writeFile(reportPath, html);
    return reportPath;
  }

  async generateIndexPage() {
    const indexPath = join(this.reportsDir, this.timestamp, 'index.html');
    
    const html = `
      <!DOCTYPE html>
      <html>
      <head>
        <title>Verification Reports Summary</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
          body { padding: 20px; }
          .summary-table { margin-top: 20px; }
        </style>
      </head>
      <body>
        <div class="container">
          <h1 class="mb-4">Verification Reports Summary</h1>
          <p class="text-muted">Generated on: ${new Date().toLocaleString()}</p>

          <table class="table table-striped summary-table">
            <thead>
              <tr>
                <th>Database</th>
                <th>Missing Columns (Old/New)</th>
                <th>SKU Mismatches (Old/New)</th>
                <th>Value Mismatches</th>
                <th>Report</th>
              </tr>
            </thead>
            <tbody>
              ${Array.from(this.databaseResults.entries()).map(([dbName, summary]) => `
                <tr>
                  <td>${dbName}</td>
                  <td>${summary.missingAttributeColumns.oldTable + summary.missingCategoryColumns.oldTable + summary.missingCommonColumns.oldTable} / 
                      ${summary.missingAttributeColumns.newTable + summary.missingCategoryColumns.newTable + summary.missingCommonColumns.newTable}</td>
                  <td>${summary.skuMismatches.missingInNew} / ${summary.skuMismatches.missingInOld}</td>
                  <td>${summary.valueMismatches.attributes + summary.valueMismatches.categories + summary.valueMismatches.commonColumns}</td>
                  <td><a href="./${dbName}.html" class="btn btn-primary btn-sm">View Report</a></td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      </body>
      </html>
    `;

    await fs.writeFile(indexPath, html);
    return indexPath;
  }
}

export default ReportGenerator; 