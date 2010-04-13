using System.Collections.Generic;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Operations;
using Rhino.Etl.Core.Pipelines;
using Xunit;

namespace Rhino.Etl.Tests.Joins
{
    public class StreamingMergeJoinFixture : BaseJoinFixture
    {
        [Fact]
        public void Should_Join_Two_Identical_Inputs()
        {
            using (StreamingInnerJoinUsersToPeopleByEmail join = new StreamingInnerJoinUsersToPeopleByEmail())
            {
                join.Left(new GenericEnumerableOperation(left))
                    .Right(new GenericEnumerableOperation(right));
                join.PrepareForExecution(new SingleThreadedPipelineExecuter());
                IEnumerable<Row> result = join.Execute(null);
                List<Row> items = new List<Row>(result);

                Assert.Equal(1, items.Count);
                Assert.Equal(3, items[0]["person_id"]);
            }
        }

        public abstract class BaseStreamingJoinUsersToPeople : StreamingMergeJoinOperation
        {
            protected override Rhino.Etl.Core.Row MergeRows(Rhino.Etl.Core.Row leftRow, Rhino.Etl.Core.Row rightRow)
            {
                Row row = new Row();
                row.Copy(leftRow);
                row["person_id"] = rightRow["id"];
                return row;
            }
        }

        public class StreamingInnerJoinUsersToPeopleByEmail : BaseStreamingJoinUsersToPeople
        {
            protected override bool MatchJoinCondition(Row leftRow, Row rightRow)
            {
                return Equals(leftRow["email"], rightRow["email"]);
            }
        }
    }
}