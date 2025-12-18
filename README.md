# WikiSearch
WikiSearch is a high-performance search and indexing engine written in golang for Wikipedia.
WikiSearch is broken up into three stages, data collection, data processing, and data presentation.
Data collection is performed via wxcrawler if you wish to crawl the Wikipedia site for page data, but alternatively, you can use wxunpacker to read data from Wikipedia's own xml dumps instead, for a much faster and more ethical approach.
Data processing is handled by wxindexer, which calcuates per-page term frequency, corpus term-frequency, both used to calcuate Term Frequency-Inverse Document Frequency (TF-IDF) scores, which are ultimately used by the search engine.
wxindexer also calculates PageRank scores, which are combined with TF-IDF scores in wxdb to provide search results to the user in the data presentation phase.

Some extra technical details:
- All three stages are separate processes, using UNIX sockets for IPC.
- Corpus TF encodings are stored in Redis, and per-page TF encodings are stored in a long .jsonl file.
- PageRank score is calculated by building a directed graph of all of Wikipedia, with edges as page references and nodes as pages. Each node is initialized with a starting score, then an algorithm iteratively traverses the graph, transferring score between nodes. This traversal is repeated until the total change in score across the graph is below a threshold.
- Mongodb stores the highest scoring pages for each term in the corpus, in order of PageRank score. User queries are broken into these terms to find search results.

### WikiSearch Data Flow Diagram
<img width="3543" height="2856" alt="WIndex_Data_Flow_dark" src="https://github.com/user-attachments/assets/ba5d598d-1575-4c0c-b3b7-5b1552d6999e" />


