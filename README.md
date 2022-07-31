npm run publish-version

rm -rf dist && tsc && rm -rf dist/node_modules && 'cp' -rf dist/ /e/Source/zerotoheroes/public-lambdas/cron-build-bgs-hero-stats/node_modules/@firestone-hs/aws-lambda-utils
