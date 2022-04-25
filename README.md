# Decentralized websites on Peerster network
Adaptation of the Peerster client, which allows upload and seamless browsing of static websites in a decentralized manner. Website files are split into content-addressed chunks, which are stored throughout the network. The DHT holds global info on which nodes store which chunks. IPNS style pointer records allow file editing without changing the underlying file's address in the DHT. The frontend, accessed through the HTTP Gateway, includes a search engine that ranks websites using the PageRank algorithm.

More details can be found in `/report.pdf`.

Project for CS-438 (Decentralized Systems Engineering) at EPFL.

## How to run
You can run the program by navigating to the gui folder and running the following command:
```console
go run mod.go start
```

Then open the file located at _gui/web/index.html_ and follow the instructions using the proxy server address found in the log of the go command above:
```console
proxy server is ready to handle requests at '<proxy_address>'
```
