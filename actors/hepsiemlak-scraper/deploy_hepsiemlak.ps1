Set-Location 'e:\shbscrapper\apify-actors\hepsiemlak-scraper'
Write-Host 'PWD:' (Get-Location)
Write-Host '--- git pull ---'
git pull origin master
Write-Host '--- git status ---'
git status
Write-Host '--- apify push --force ---'
apify push --force
