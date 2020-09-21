using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using SiteSearch;

namespace SiteSearchTest
{
    [TestFixture]
    public class Class1
    {
        [Test]
        public async Task DoIndex()
        {
            var file = Path.GetTempFileName();
            try
            {
                var cut = new Index(file);
                var pages = new IPageData[]
                {
                    new PageData("TestPage1.html"),
                    new PageData("TestPage2.html"),
                    new PageData("TestPage3.html"),
                    new PageData("TestPage4.html")
                };
                await cut.DoIndexAsync(pages, CancellationToken.None);

                using (var actual = new FileStream(file, FileMode.Open))
                using (var expected = new FileStream("IndexData.dat", FileMode.Open))
                {
                    FileAssert.AreEqual(expected, actual);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Test]
        public async Task SearchAsync_EmptySearch_GivesEmptyResult()
        {
            var cut = new Index("IndexData.dat");
            var result = await cut.SearchAsync("", CancellationToken.None);

            CollectionAssert.IsEmpty(result);
        }

        [Test]
        public async Task SearchAsync_NullSearch_GivesEmptyResult()
        {
            var cut = new Index("IndexData.dat");
            var result = await cut.SearchAsync("", CancellationToken.None);

            CollectionAssert.IsEmpty(result);
        }

        [Test]
        public async Task SearchAsync_ValidSearch_GivesResult()
        {
            var cut = new Index("IndexData.dat");
            var result = await cut.SearchAsync("de", CancellationToken.None);

            CollectionAssert.AreEqual(new[] { "TestPage1.html", "TestPage4.html", "TestPage3.html" }, result);
        }

        [Test]
        public async Task SearchAsync_WordNotPresent_GivesEmptyResult()
        {
            var cut = new Index("IndexData.dat");
            var result = await cut.SearchAsync("koolstofdatering", CancellationToken.None);

            CollectionAssert.IsEmpty(result);
        }

        private class PageData : IPageData
        {
            public PageData(string url)
            {
                Url = url;
            }

            public string Url { get; }

            public Stream GetPageStream()
            {
                return new FileStream(Url, FileMode.Open);
            }
        }
    }
}