 find ./aa/|grep host|cpio -ov|gzip > 1.tar.gz
 cat 1.tar.gz|gunzip|cpio -itv
