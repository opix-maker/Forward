name: Build IMDb Discovery Engine Data

on:
  workflow_dispatch:
  schedule:
    - cron: '0 0 * * *'

permissions:
  contents: write

jobs:
  build:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    
    # 不再需要 working-directory，让所有路径都从仓库根目录开始计算
    # defaults:
    #   run:
    #     working-directory: ./imdb-data-platform

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Synchronize with remote branch
        run: git pull --rebase

      - name: Set up Node.js
        uses: actions/setup-node@v4
        with:
          node-version: '20'
          cache: 'npm'
          cache-dependency-path: imdb-data-platform/package.json

      - name: Install dependencies
        
        run: npm install --prefix ./imdb-data-platform

      - name: Run build script
        env:
          TMDB_ACCESS_TOKEN_V4: ${{ secrets.TMDB_ACCESS_TOKEN_V4 }}
        
        run: node imdb-data-platform/build.js

      - name: Commit and push if changed
        uses: stefanzweifel/git-auto-commit-action@v5
        with:
          commit_message: 'chore(data): Auto-update IMDb data marts'
          file_pattern: 'imdb-data-platform/dist/*.json'
