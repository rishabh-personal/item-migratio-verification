import fs from 'fs/promises';
import { createWriteStream } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import chalk from 'chalk';

const __dirname = dirname(fileURLToPath(import.meta.url));

// Create logs directory if it doesn't exist
const logsDir = join(__dirname, '../logs');
try {
  await fs.mkdir(logsDir, { recursive: true });
} catch (error) {
  console.error('Error creating logs directory:', error);
}

// Create a write stream for the log file with current timestamp
const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
const logFile = createWriteStream(join(logsDir, `verification-${timestamp}.log`), { flags: 'a' });

// Strip ANSI color codes for file logging
const stripAnsi = (str) => str.replace(/\u001b\[\d+m/g, '');

function formatMessage(level, message) {
  const timestamp = new Date().toISOString();
  return `[${timestamp}] [${level}] ${message}`;
}

const logger = {
  info: (message) => {
    const consoleMsg = chalk.blue(formatMessage('INFO', message));
    const fileMsg = stripAnsi(formatMessage('INFO', message));
    
    console.log(consoleMsg);
    logFile.write(fileMsg + '\n');
  },

  success: (message) => {
    const consoleMsg = chalk.green(formatMessage('SUCCESS', message));
    const fileMsg = stripAnsi(formatMessage('SUCCESS', message));
    
    console.log(consoleMsg);
    logFile.write(fileMsg + '\n');
  },

  warning: (message) => {
    const consoleMsg = chalk.yellow(formatMessage('WARNING', message));
    const fileMsg = stripAnsi(formatMessage('WARNING', message));
    
    console.log(consoleMsg);
    logFile.write(fileMsg + '\n');
  },

  error: (message, error = null) => {
    const errorDetails = error ? `\n${error.stack || error}` : '';
    const consoleMsg = chalk.red(formatMessage('ERROR', message + errorDetails));
    const fileMsg = stripAnsi(formatMessage('ERROR', message + errorDetails));
    
    console.error(consoleMsg);
    logFile.write(fileMsg + '\n');
  },

  table: (data, message = '') => {
    if (message) {
      const consoleMsg = formatMessage('INFO', message);
      const fileMsg = stripAnsi(formatMessage('INFO', message));
      
      console.log(consoleMsg);
      logFile.write(fileMsg + '\n');
    }
    
    console.table(data);
    logFile.write(JSON.stringify(data, null, 2) + '\n');
  },

  close: () => {
    logFile.end();
  }
};

export default logger; 