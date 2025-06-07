/**
 * 通用辅助函数
 */
import fs from 'fs/promises';
import path from 'path';

// 异步等待函数
export const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// 解析IMDb页面上的评分字符串
export function parseImdbRating(ratingString) {
  if (!ratingString) return 0;
  const match = ratingString.match(/^(\d+\.\d+)/);
  return match ? parseFloat(match[1]) : 0;
}

// 确保目录存在，如果不存在则创建
export async function ensureDir(dirPath) {
    try {
        await fs.access(dirPath);
    } catch (error) {
        if (error.code === 'ENOENT') {
            await fs.mkdir(dirPath, { recursive: true });
        } else {
            throw error;
        }
    }
}

// 将数据写入指定的JSON文件
export async function writeJsonFile(filePath, data) {
    const dir = path.dirname(filePath);
    await ensureDir(dir);
    await fs.writeFile(filePath, JSON.stringify(data, null, 2));
}
