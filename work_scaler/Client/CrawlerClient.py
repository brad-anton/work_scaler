"""CrawlerClient.py
@brad_anton

When a job is published from the server, a greenlet
is spawned and the URL provided in the job message is fetched
and its body text is retrieved. Body text is then run through
Sumy to summarize the content on the site. The content is 
returned to the server for futher processing.

"""
from Client import Client
"""geventhttpclient does not follow redirects so we patch 
urllib2 to do the crawling in a friendly way
"""
import geventhttpclient.httplib
geventhttpclient.httplib.patch()

from urllib2 import urlopen

from uuid import uuid4

# All for Natural Language
from sumy.parsers.html import HtmlParser
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lex_rank import LexRankSummarizer as Summarizer
from sumy.nlp.stemmers import Stemmer
from sumy.utils import get_stop_words

LANGUAGE = "english"

class CrawlerClient(Client):
    def do_work(self, worker_id, work):
        url = work
        """Greenlet to fetch analyze URL content
        """
        print '[+] {0}: Starting crawl of {1}'.format(worker_id, url)

        """Using urllib2 via geventhttpclient. Selenium with 
        PhantomJS or a real browser would be probably better
        but slower and more expensive. Could have also used
        scrapy, but thats way to heavy for this use-case."""
        body = urlopen(url).read()

        """Using Sumy (built on nltk) for page summaries since
        it supports a number of ranking algorithms. It's not
        perfect though, it was written for czech and so its 
        missing some important English-specific things (e.g.
        bonus/significant words for Edmundson Summarizers)

        https://pypi.python.org/pypi/sumy

        TextBlob might be a better alternative, but it didn't
        seem to provide overall summary information. 

        https://textblob.readthedocs.org/en/latest/
        """
        parser = HtmlParser.from_string(body, None, Tokenizer(LANGUAGE))
        stemmer = Stemmer(LANGUAGE)

        summarizer = Summarizer(stemmer)
        summarizer.stop_words = get_stop_words(LANGUAGE)

        words = []
        for sentence in summarizer(parser.document, 10):
            words = str(sentence).split()

        # Send the results
        self.work_done(worker_id, words)

if __name__ == '__main__':
    c = CrawlerClient()
    c.run()
