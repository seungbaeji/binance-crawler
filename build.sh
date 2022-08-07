#! /bin/sh
for name in link_crawler downloader validator kline_pusher; do 
    docker buildx build $PWD/$name \
        --push \
        --platform linux/arm64,linux/amd64 \
        --tag docker.kube.home/apps/binance-crawler/$name \
        --file $PWD/$name/Dockerfile
    echo "BUILD $name IS DONE"
done