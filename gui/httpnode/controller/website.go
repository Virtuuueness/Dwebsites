package controller

import (
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/peer"

	"golang.org/x/net/html"
)

type redirectServer struct {
	peer       peer.Peer
	log        *zerolog.Logger
	localCache map[string]uint
}

const error404File = "web/404.html"

const topFolder = "Peersteer"

func NewRedirectServer(peer peer.Peer, log *zerolog.Logger) redirectServer {
	return redirectServer{
		peer:       peer,
		log:        log,
		localCache: make(map[string]uint),
	}
}

func (re *redirectServer) CreateOrModifyHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			re.uploadWebsite(w, r)
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

func (re *redirectServer) RedirectHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			fullURL := strings.TrimPrefix(r.URL.Path[1:], "www.")
			// Get site's folder (either locally or remote and serve the right file)
			http.ServeFile(w, r, re.getWebsiteAndRedirectLinks(fullURL, fmt.Sprintf("http://%s/", r.Host)))
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

func (re *redirectServer) uploadWebsite(w http.ResponseWriter, r *http.Request) {
	err := r.ParseMultipartForm(1024 * 1024 * 16)
	if err != nil {
		http.Error(w, "failed to read body: "+err.Error(), http.StatusInternalServerError)
		return
	}
	websiteName := strings.TrimPrefix(r.MultipartForm.Value["Name"][0], "www.")
	rootPath := filepath.Join(os.TempDir(), topFolder, websiteName)
	err = os.RemoveAll(rootPath)
	if err != nil {
		http.Error(w, "failed to create top level folder: "+err.Error(), http.StatusInternalServerError)
		return
	}
	err = os.MkdirAll(rootPath, os.ModePerm)
	if err != nil {
		http.Error(w, "failed to create top level folder: "+err.Error(), http.StatusInternalServerError)
		return
	}
	for _, fileHeader := range r.MultipartForm.File["Files"] {
		file, err := fileHeader.Open()
		if err != nil {
			http.Error(w, "failed to read file: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer file.Close()
		reg, _ := regexp.Compile(`filename="(.*)"`)
		match := reg.FindStringSubmatch(fileHeader.Header.Get("Content-Disposition"))
		if len(match) != 2 {
			http.Error(w, "failed to read filename", http.StatusInternalServerError)
			return
		}
		cleanPath := filepath.Clean(match[1])
		splittedPath := strings.Split(cleanPath, "/")
		splittedPath[0] = websiteName
		path := filepath.Join(os.TempDir(), topFolder, strings.Join(splittedPath, "/"))
		err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
		if err != nil {
			http.Error(w, "failed to create sub folder: "+err.Error(), http.StatusInternalServerError)
			return
		}
		out, err := os.Create(path)
		if err != nil {
			http.Error(w, "failed to create file on server: "+err.Error(), http.StatusInternalServerError)
			return
		}
		_, err = io.Copy(out, file)
		if err != nil {
			http.Error(w, "failed to copy file on server: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		http.Error(w, "failed to generate private key: "+err.Error(), http.StatusInternalServerError)
		return
	}
	mh := re.peer.Resolve(websiteName)
	shouldTag := true
	sequence := uint(0)
	if mh != "" {
		if pKey, ok := re.peer.GetRecordSignature(mh); ok {
			privateKey = pKey
			shouldTag = false
			fetchedRecord, ok2 := re.peer.FetchPointerRecord(mh)
			if ok2 {
				sequence = fetchedRecord.Sequence + 1
			} else {
				http.Error(w, "record linked to existing website not found", http.StatusInternalServerError)
				return
			}
		} else {
			http.Error(w, "website already exists and is not own by this user", http.StatusInternalServerError)
			return
		}
	}
	recordHash, err := re.peer.CreateAndPublishFolderRecord(rootPath, websiteName, privateKey, sequence, 100)
	if err != nil {
		http.Error(w, "failed to create and publish record: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if shouldTag {
		err = re.peer.Tag(websiteName, recordHash)
		if err != nil {
			http.Error(w, "failed to tag record: "+err.Error(), http.StatusInternalServerError)
			return
		}
		re.peer.SetRecordSignature(recordHash, privateKey)
	}
	re.addPageRankLinks(rootPath, websiteName)
}

// Get files remotely and decorate local folder's links or from cache if same version than peerster
func (r *redirectServer) getWebsiteAndRedirectLinks(fullURL string, hostUrl string) string {
	// Extract domain name to get site's folder
	reg, err := regexp.Compile(`(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9][a-z0-9-]{0,61}[a-z0-9]`)
	if err != nil || !reg.MatchString(fullURL) {
		r.log.Info().Msg("Website not found using regex")
		return error404File
	}
	websiteName := reg.FindString(fullURL)
	addr := r.peer.Resolve(websiteName)
	if addr == "" {
		r.log.Error().Msg("website not found " + websiteName)
		return error404File
	}
	fetchedRecord, ok := r.peer.FetchPointerRecord(addr)
	if !ok {
		r.log.Error().Msg("website not found " + websiteName)
		return error404File
	}
	// Last version is already in cache
	if seq, ok := r.localCache[websiteName]; ok && seq == fetchedRecord.Sequence {
		return filepath.Join(os.TempDir(), topFolder, fullURL)
	}
	_, err = r.peer.ReconstructFolderFromRecord(filepath.Join(os.TempDir(), topFolder), fetchedRecord)
	if err != nil {
		r.log.Error().Msg("could not reconstruct folder from pointer: " + addr)
		return error404File
	}
	decorateFolder(filepath.Join(os.TempDir(), topFolder, websiteName), hostUrl)
	r.localCache[websiteName] = fetchedRecord.Sequence
	return filepath.Join(os.TempDir(), topFolder, fullURL)
}

// Decorate all HTML file in a folder by changing link to localhost redirect
func decorateFolder(path string, hostUrl string) error {
	var files []string
	err := filepath.Walk(path,
		func(path string, info os.FileInfo, err error) error {
			if info != nil && !info.IsDir() && strings.HasSuffix(strings.ToLower(info.Name()), ".html") {
				files = append(files, path)
			}
			return err
		})
	if err != nil {
		return err
	}
	for _, f := range files {
		decorateHTML(f, hostUrl)
	}
	return nil
}

// Decorate the HTML file at path with redirected links to localhost
func decorateHTML(path string, hostUrl string) error {
	text, err := fileToString(path)
	if err != nil {
		return err
	}
	links, err := extractHrefFromContent(text)
	if err != nil {
		return err
	}
	return stringToFile(path, replaceLinks(text, links, hostUrl))
}

func (r *redirectServer) addPageRankLinks(path string, websiteName string) error {

	var files []string
	err := filepath.Walk(path,
		func(path string, info os.FileInfo, err error) error {
			if info != nil && !info.IsDir() && strings.HasSuffix(strings.ToLower(info.Name()), ".html") {
				files = append(files, path)
			}
			return err
		})
	if err != nil {
		return err
	}
	for _, f := range files {
		text, err := fileToString(f)
		if err != nil {
			return err
		}
		links, err := extractHrefFromContent(text)
		if err != nil {
			return err
		}
		fmt.Printf("links: %v\n", links)
		for _, link := range links {
			l := strings.TrimPrefix(link, "www.")

			fmt.Println("Add link : ", websiteName, " -> ", l)

			err = r.peer.AddLink(websiteName, l)
			fmt.Printf("\"added link \": %v\n", "added link ")
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func fileToString(path string) (string, error) {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func stringToFile(path string, content string) error {
	return ioutil.WriteFile(path, []byte(content), 0644)
}

// Replace all links (currentUrls) in inputContent with localhost redirected one
func replaceLinks(inputContent string, currentUrls []string, hostUrl string) string {
	var replaceArr []string
	for _, c := range currentUrls {

		replaceArr = append(replaceArr, c)
		if c[:5] == "https" {
			replaceArr = append(replaceArr, hostUrl+c[8:])
		} else if c[:4] == "http" {
			replaceArr = append(replaceArr, hostUrl+c[7:])
		} else {
			replaceArr = append(replaceArr, c)
		}
	}
	r := strings.NewReplacer(replaceArr...)
	return r.Replace(inputContent)
}

// Get the url from an html token
func getHrefFromToken(t html.Token) (ok bool, href string) {
	for _, a := range t.Attr {
		if a.Key == "href" {
			href = a.Val
			ok = true
		}
	}
	return
}

// Get all urls from an html file content
func extractHrefFromContent(content string) ([]string, error) {
	var results []string
	z := html.NewTokenizer(strings.NewReader(content))
	for {
		tt := z.Next()
		switch {
		case tt == html.ErrorToken:
			return results, nil
		case tt == html.StartTagToken:
			t := z.Token()
			if t.Data != "a" {
				continue
			}
			ok, url := getHrefFromToken(t)
			if !ok {
				continue
			}
			results = append(results, url)
		}
	}
}
