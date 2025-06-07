// 通用辅助函数

// 延迟函数，用于在请求之间添加礼貌性的等待
export const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// 将IMDb评分字符串（如"8.7/10 from 2.9M users"）解析为数字
export function parseImdbRating(ratingString) {
  if (!ratingString) return 0;
  const match = ratingString.match(/^(\d+\.\d+)/);
  return match ? parseFloat(match[1]) : 0;
}

// 将IMDb时长字符串（如"1h 30m"）解析为分钟数
export function parseImdbDuration(durationString) {
    if (!durationString) return 0;
    let totalMinutes = 0;
    const hourMatch = durationString.match(/(\d+)h/);
    const minMatch = durationString.match(/(\d+)m/);
    if (hourMatch) totalMinutes += parseInt(hourMatch[1]) * 60;
    if (minMatch) totalMinutes += parseInt(minMatch[1]);
    return totalMinutes;
}
