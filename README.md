# s3pd 
Uses aws crt cpp lib for high performance

to build run:
```
$ cmake .
$ make
```

CLI will then be installed at:
```
./s3pd
```

# Mac osx  install
Add to ~/.zshrc
```
# Add zlib
export LDFLAGS="-L/opt/homebrew/opt/zlib/lib"
export CPPFLAGS="-I/opt/homebrew/opt/zlib/include"
export PKG_CONFIG_PATH="/opt/homebrew/opt/zlib/lib/pkgconfig"
export PATH=/opt/homebrew/opt/zlib/lib:/opt/homebrew/opt/zlib/include:$PATH

# Add libcurl
export LDFLAGS="-L/opt/homebrew/opt/curl/lib"
export CPPFLAGS="-I/opt/homebrew/opt/curl/include"
export PKG_CONFIG_PATH="/opt/homebrew/opt/curl/lib/pkgconfig"
export PATH=/opt/homebrew/opt/curlpp/include/curlpp:$PATH
export PATH=/opt/homebrew/opt/curlpp/lib:$PATH
export CURL_LIBRARY=/opt/homebrew/opt/curl/lib
export CURL_INCLUDE_DIR=/opt/homebrew/opt/curl/include
```

# Deps
zlib
curl
boost
