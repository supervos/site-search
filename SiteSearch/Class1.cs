using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using HtmlAgilityPack;

namespace SiteSearch
{
    public interface IPageData
    {
        string Url { get; }

        Stream GetPageStream();
    }

    public sealed class Index
    {
        /* file setup:
         * - newlines or spaces do not count
         * - L = long, S = short, W = array of characters
         * - items between accolades are groups, accolades are not present
         * - a plus sign means that it can be repeated 0, 1 or multiple times
         * - the file is structured as a binary search
         *   ° the [lowerWord] points to the stream position where the word before the current word starts
         *   ° the [upperWord] points to the stream position where the word after the current word starts
         *
         * [urlStart:L]
         * (
         *   [wordLength:S][word:W]
         *   [lowerWord:L][upperWord:L]
         *   [pageCount:S]
         *   (
         *     [urlIndex:S][wordCount:S]
         *     ([wordIndex:S])+
         *   )+
         * )+
         * [urlCount:S]
         * [urlPos:L]+
         * ([urlWordLength:S][urlWord:W])+
         */

        public Index(string file)
        {
            File = file;
            Encoding = Encoding.UTF8;
            Comparer = StringComparer.OrdinalIgnoreCase;
        }

        internal StringComparer Comparer { get; }

        internal Encoding Encoding { get; }

        internal string File { get; }

        public Task DoIndexAsync(IEnumerable<IPageData> pages, CancellationToken token)
        {
            var helper = new CreateIndexHelper(this);

            return helper.DoIndexAsync(pages, token);
        }

        public Task<IList<string>> SearchAsync(string query, CancellationToken token)
        {
            var helper = new SearchIndexHelper(this);
            using (var timedToken = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
            using (var combinedToken = CancellationTokenSource.CreateLinkedTokenSource(token, timedToken.Token))
            {
                return helper.SearchAsync(query, combinedToken.Token);
            }
        }
    }

    internal sealed class CreateIndexHelper
    {
        private readonly Index _index;

        public CreateIndexHelper(Index index)
        {
            _index = index;
        }

        public async Task DoIndexAsync(IEnumerable<IPageData> pages, CancellationToken token)
        {
            var data = new SortedList<string, Dictionary<string, List<short>>>(_index.Comparer);
            foreach (var page in pages)
            {
                var content = new HtmlDocument();
                content.Load(page.GetPageStream());

                IndexPage(content.DocumentNode, page.Url, data, token);
            }

            await SaveDataAsync(data, token);
        }

        private void IndexPage(HtmlNode node, string file, SortedList<string, Dictionary<string, List<short>>> data, CancellationToken token)
        {
            short wordPosition = 0;
            while (node != null)
            {
                if (CanParse())
                {
                    if (node.HasChildNodes)
                    {
                        node = node.FirstChild;
                        continue;
                    }

                    ParseNode();
                }

                token.ThrowIfCancellationRequested();

                if (node.NextSibling != null)
                {
                    node = node.NextSibling;
                    continue;
                }

                while (node != null)
                {
                    node = node.ParentNode;
                    if (node?.NextSibling != null)
                    {
                        node = node.NextSibling;
                        break;
                    }
                }
            }

            bool CanParse()
            {
                switch (node.Name.ToLowerInvariant())
                {
                    case "head":
                    case "script":
                    case "#comment":
                        return false;

                    case "a":
                        ParseNode();
                        return true;

                    default:
                        return true;
                }
            }

            void ParseNode()
            {
                switch (node.Name.ToLowerInvariant())
                {
                    case "img":
                        ParseText(node.GetAttributeValue("title", null));
                        ParseText(node.GetAttributeValue("alt", null));
                        return;

                    case "a":
                        ParseText(node.GetAttributeValue("title", null));
                        break;
                }

                ParseText(node.InnerText);
            }

            void ParseText(string contentText)
            {
                if (string.IsNullOrWhiteSpace(contentText))
                {
                    return;
                }

                var wordStart = char.IsLetterOrDigit(contentText, 0) ? 0 : 1;
                for (var i = 1; i < contentText.Length; i += 1)
                {
                    if (char.IsLetterOrDigit(contentText, i))
                    {
                        continue;
                    }

                    if (wordStart == i)
                    {
                        wordStart = i + 1;
                        continue;
                    }

                    IndexWord(contentText, wordStart, i);
                    wordStart = i + 1;
                }

                if (wordStart < contentText.Length)
                {
                    IndexWord(contentText, wordStart, contentText.Length);
                }
            }

            void IndexWord(string contentText, int start, int end)
            {
                var word = contentText.Substring(start, end - start);
                if (!data.TryGetValue(word, out var fileData))
                {
                    fileData = new Dictionary<string, List<short>>();
                    data[word] = fileData;
                }

                if (!fileData.TryGetValue(file, out var wordIndex))
                {
                    wordIndex = new List<short>();
                    fileData[file] = wordIndex;
                }

                wordIndex.Add(wordPosition++);
            }
        }

        private async Task ProcessQueueItem(
            Stream stream,
            SortedList<string, Dictionary<string, List<short>>> data,
            Queue<(int, int, long)> queue,
            Dictionary<string, short> urls,
            CancellationToken token)
        {
            var (lower, higher, write) = queue.Dequeue();
            if (write > 0)
            {
                var currentPos = stream.Position;
                stream.Position = write;
                await WriteLongAsync(stream, currentPos, token);
                stream.Position = currentPos;
            }

            var pos = (higher - lower) / 2 + lower;
            var word = data.Keys[pos];
            await WriteStringAsync(stream, word, token);
            if (pos > lower)
            {
                queue.Enqueue((lower, pos - 1, stream.Position));
            }

            await WriteLongAsync(stream, -1, token);

            if (pos < higher)
            {
                queue.Enqueue((pos + 1, higher, stream.Position));
            }

            await WriteLongAsync(stream, -1, token);

            await WriteDataAsync(stream, data[word], urls, token);
        }

        private async Task SaveDataAsync(SortedList<string, Dictionary<string, List<short>>> data, CancellationToken token)
        {
            // Queue contains the segments that still need to be written,
            // the streamReference records the position in the stream that points to this segment.
            //   (lowerBound, upperBound, streamReference)
            var queue = new Queue<(int, int, long)>();
            var urls = new Dictionary<string, short>();
            if (data.Count > 0)
            {
                queue.Enqueue((0, data.Count - 1, -1));
            }

            using (var stream = new FileStream(_index.File, FileMode.Create, FileAccess.ReadWrite))
            {
                await WriteLongAsync(stream, 0, token);
                while (queue.Count > 0)
                {
                    token.ThrowIfCancellationRequested();
                    await ProcessQueueItem(stream, data, queue, urls, token);
                }

                await WriteUrls(stream, urls, token);
            }
        }

        private async Task WriteDataAsync(
            Stream stream, Dictionary<string, List<short>> data,
            Dictionary<string, short> urls,
            CancellationToken token)
        {
            await WriteShortAsync(stream, (short)data.Count, token);
            foreach (var pair in data)
            {
                if (!urls.TryGetValue(pair.Key, out var urlIndex))
                {
                    urlIndex = (short)urls.Count;
                    urls[pair.Key] = urlIndex;
                }

                await WriteShortAsync(stream, urlIndex, token);
                await WriteShortAsync(stream, (short)pair.Value.Count, token);
                foreach (var value in pair.Value)
                {
                    await WriteShortAsync(stream, value, token);
                }
            }
        }

        private async Task WriteLongAsync(Stream stream, long value, CancellationToken token)
        {
            await stream.WriteAsync(BitConverter.GetBytes(value), 0, 8, token);
        }

        private async Task WriteShortAsync(Stream stream, short value, CancellationToken token)
        {
            await stream.WriteAsync(BitConverter.GetBytes(value), 0, 2, token);
        }

        private async Task WriteStringAsync(Stream stream, string value, CancellationToken token)
        {
            var bytes = _index.Encoding.GetBytes(value);
            await stream.WriteAsync(BitConverter.GetBytes((short)bytes.Length), 0, 2, token);
            await stream.WriteAsync(bytes, 0, bytes.Length, token);
        }

        private async Task WriteUrls(Stream stream, Dictionary<string, short> urls, CancellationToken token)
        {
            var urlStart = stream.Position;
            stream.Position = 0;
            await WriteLongAsync(stream, urlStart, token);
            stream.Position = urlStart;
            await WriteShortAsync(stream, (short)urls.Count, token);
            urlStart = stream.Position;

            // write placeholders for the word indices
            for (var i = 0; i < urls.Count; i += 1)
            {
                await WriteLongAsync(stream, 0, token);
            }

            token.ThrowIfCancellationRequested();

            // write the urls
            foreach (var pair in urls)
            {
                var curPos = stream.Position;
                stream.Position = urlStart + pair.Value * 8;
                await WriteLongAsync(stream, curPos, token);
                stream.Position = curPos;
                await WriteStringAsync(stream, pair.Key, token);
            }
        }
    }

    internal sealed class SearchIndexHelper
    {
        private readonly Index _index;

        public SearchIndexHelper(Index index)
        {
            _index = index;
        }

        public async Task<IList<string>> SearchAsync(string query, CancellationToken token)
        {
            if (string.IsNullOrWhiteSpace(query))
            {
                return Array.Empty<string>();
            }

            using (var stream = new FileStream(_index.File, FileMode.Open, FileAccess.Read))
            {
                var words = query.Split(' ');
                var results = new Dictionary<short, double>();
                foreach (var word in words.Distinct())
                {
                    token.ThrowIfCancellationRequested();

                    stream.Position = 8;

                    var r = await SearchWordAsync(stream, word, token);
                    if (r == null)
                    {
                        continue;
                    }

                    double sum = r.Values.Sum();
                    foreach (var pair in r)
                    {
                        _ = results.TryGetValue(pair.Key, out var popularity);
                        popularity += pair.Value / sum;
                        results[pair.Key] = popularity;
                    }
                }

                var convertToUrl = results
                    .OrderByDescending(m => m.Value)
                    .ThenBy(m => m.Key)
                    .Select(m => m.Key);

                var posBuffer = new byte[8];
                stream.Position = 0;
                await ReadBufferAsync(stream, posBuffer, token);
                var urlStart = BitConverter.ToInt64(posBuffer, 0) + 2;
                var urlResults = new List<string>(results.Count);
                foreach (var index in convertToUrl)
                {
                    stream.Position = urlStart + index * 8;
                    await ReadBufferAsync(stream, posBuffer, token);
                    stream.Position = BitConverter.ToInt64(posBuffer, 0);
                    var url = await ReadRawWord(stream, token);
                    urlResults.Add(url);
                }

                return urlResults;
            }
        }

        private async Task ReadBufferAsync(Stream input, byte[] data, CancellationToken token)
        {
            var index = 0;
            do
            {
                index += await input.ReadAsync(data, index, data.Length - index, token);
            } while (index < data.Length);
        }

        private async Task<string> ReadRawWord(Stream stream, CancellationToken token)
        {
            var numBuffer = new byte[2];
            await ReadBufferAsync(stream, numBuffer, token);
            var wordBuffer = new byte[BitConverter.ToInt16(numBuffer, 0)];
            await ReadBufferAsync(stream, wordBuffer, token);
            return _index.Encoding.GetString(wordBuffer);
        }

        private async Task<(string word, long prevPos, long nextPos)> ReadSearchWordAsync(Stream stream, CancellationToken token)
        {
            var word = await ReadRawWord(stream, token);
            var numBuffer = new byte[8];

            await ReadBufferAsync(stream, numBuffer, token);
            var prevPos = BitConverter.ToInt64(numBuffer, 0);
            await ReadBufferAsync(stream, numBuffer, token);
            var nextPos = BitConverter.ToInt64(numBuffer, 0);
            return (word, prevPos, nextPos);
        }

        private async Task<Dictionary<short, int>> SearchWordAsync(Stream stream, string needle, CancellationToken token)
        {
            while (true)
            {
                token.ThrowIfCancellationRequested();

                var (stackWord, prevPos, nextPos) = await ReadSearchWordAsync(stream, token);
                var compResult = _index.Comparer.Compare(needle, stackWord);
                if (compResult != 0)
                {
                    nextPos = compResult < 0 ? prevPos : nextPos;
                    if (nextPos <= 0)
                    {
                        // Word not found, not able to continue the search
                        return null;
                    }

                    stream.Position = nextPos;
                    continue;
                }

                // Read data
                var numBuffer = new byte[2];
                await ReadBufferAsync(stream, numBuffer, token);
                var count = BitConverter.ToInt16(numBuffer, 0);
                var results = new Dictionary<short, int>(count);
                for (var i = 0; i < count; i += 1)
                {
                    await ReadBufferAsync(stream, numBuffer, token);
                    var urlPosition = BitConverter.ToInt16(numBuffer, 0);
                    await ReadBufferAsync(stream, numBuffer, token);
                    var occurrences = BitConverter.ToInt16(numBuffer, 0);
                    stream.Position += occurrences * 2;
                    results.Add(urlPosition, occurrences);
                }

                return results;
            }
        }
    }
}