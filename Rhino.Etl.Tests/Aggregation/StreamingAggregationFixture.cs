using System.Linq;
using Rhino.Etl.Core.Operations;

namespace Rhino.Etl.Tests.Aggregation
{
    using System.Collections.Generic;
    using Core;
    using Xunit;

    public class StreamingAggregationFixture
    {
        [Fact]
        public void Should_Yield_Nothing_When_No_Rows_Input()
        {
            var op = new RowCountByColorOperation();
            var result = op.Execute(new Row[0]).ToList();
            Assert.Equal(0, result.Count);
        }

        [Fact]
        public void Should_Aggregate_A_Single_Row()
        {
            var items = new List<Row>
                {
                    new Row {{"color", "red"}},
                };

            var op = new RowCountByColorOperation();
            var result = op.Execute(items).ToList();

            Assert.Equal(1, result.Count);  // "Should yield a single row."
            Assert.Equal(1, result[0]["count"]); // "Should have a 'count' attribute equal to 1"
            Assert.Equal("red", result[0]["color"]); // "Should copy the group by key value to the aggregate row."
        }

        [Fact]
        public void Should_Aggregate_Multiple_Groups()
        {
            var items = new List<Row>
                {
                    new Row {{"color", "red"}}, new Row {{"color", "red"}},
                    new Row {{"color", "green"}}, new Row {{"color", "green"}}, new Row {{"color", "green"}},
                    new Row {{"color", "blue"}}, new Row {{"color", "blue"}}, new Row {{"color", "blue"}}, new Row {{"color", "blue"}},
                };

            var op = new RowCountByColorOperation();
            var result = op.Execute(items).ToList();

            Assert.Equal(3, result.Count);

            Assert.Equal(2, result[0]["count"]);
            Assert.Equal("red", result[0]["color"]);

            Assert.Equal(3, result[1]["count"]);
            Assert.Equal("green", result[1]["color"]);

            Assert.Equal(4, result[2]["count"]);
            Assert.Equal("blue",result[2]["color"]);
        }

        [Fact]
        public void Should_Aggregate_An_Out_Of_Order_Key_As_Its_Own_Aggregate()
        {
            var items = new List<Row>
                {
                    new Row {{"color", "red"}}, new Row {{"color", "red"}},
                    new Row {{"color", "green"}}, new Row {{"color", "green"}}, new Row {{"color", "green"}},
                    new Row {{"color", "blue"}}, new Row {{"color", "blue"}}, new Row {{"color", "blue"}}, new Row {{"color", "blue"}},
                    new Row {{"color", "red"}}, new Row {{"color", "red"}}, new Row {{"color", "red"}}, new Row {{"color", "red"}},
                };

            var op = new RowCountByColorOperation();
            var result = op.Execute(items).ToList();

            Assert.Equal(4, result.Count);

            Assert.Equal(2, result[0]["count"]);
            Assert.Equal("red",result[0]["color"]);

            Assert.Equal(3,result[1]["count"]);
            Assert.Equal("green",result[1]["color"]);

            Assert.Equal(4,result[2]["count"]);
            Assert.Equal("blue",result[2]["color"]);

            Assert.Equal(4,result[3]["count"]);
            Assert.Equal("red",result[3]["color"]);
        }

        [Fact]
        public void Should_Add_Each_GroupByKey_To_The_Aggregate_Row()
        {
            var items = new List<Row>
                {
                    new Row {{"color", "red"}, {"size", "S"}}, new Row {{"color", "red"}, {"size", "S"}},
                    new Row {{"color", "red"}, {"size", "L"}}, new Row {{"color", "red"}, {"size", "L"}},
                    new Row {{"color", "green"}, {"size", "S"}}, new Row {{"color", "green"}, {"size", "S"}},
                    new Row {{"color", "green"}, {"size", "L"}}, new Row {{"color", "green"}, {"size", "L"}},
                };

            var op = new RowCountByColorAndSizeOperation();
            var result = op.Execute(items).ToList();

            Assert.Equal(4, result.Count);

            Assert.Equal(2, result[0]["count"]);
            Assert.Equal("red",result[0]["color"]);
            Assert.Equal("S",result[0]["size"]);

            Assert.Equal(2,result[1]["count"]);
            Assert.Equal("red",result[1]["color"]);
            Assert.Equal("L",result[1]["size"]);

            Assert.Equal(2,result[2]["count"]);
            Assert.Equal("green",result[2]["color"]);
            Assert.Equal("S",result[2]["size"]);

            Assert.Equal(2,result[3]["count"]);
            Assert.Equal("green",result[3]["color"]);
            Assert.Equal("L",result[3]["size"]);
        }

        public abstract class StreamingRowCountOperation : StreamingAggregationOperation
        {
            protected override void Accumulate(Row row, Row aggregate)
            {
                if (aggregate["count"] == null)
                {
                    aggregate["count"] = 0;
                }

                int count = (int)aggregate["count"];
                aggregate["count"] = count + 1;
            }
        }

        public class RowCountByColorOperation : StreamingRowCountOperation
        {
            protected override string[] GetColumnsToGroupBy()
            {
                return new string[] { "color" };
            }
        }

        public class RowCountByColorAndSizeOperation : StreamingRowCountOperation
        {
            protected override string[] GetColumnsToGroupBy()
            {
                return new string[] { "color", "size" };
            }
        }
    }
}