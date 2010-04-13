using System.Collections.Generic;

namespace Rhino.Etl.Core.Operations
{
    /// <summary>A streaming, non-blocking, aggregation operation.</summary>
    public abstract class StreamingAggregationOperation : AbstractAggregationOperation
    {
        /// <summary>Executes this operation</summary>
        /// <param name="rows">The rows.</param>
        /// <returns>The aggregated rows.</returns>
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            string[] groupBy = GetColumnsToGroupBy();

            Row currentAggregate = null;
            ObjectArrayKeys currentKey = null;

            foreach (Row row in rows)
            {
                ObjectArrayKeys key = row.CreateKey(groupBy);
                if (key.Equals(currentKey))
                {
                    Accumulate(row, currentAggregate);
                    continue;
                }

                if (currentKey != null)
                {
                    FinishAggregation(currentAggregate);
                    yield return currentAggregate;
                }

                currentKey = key;
                currentAggregate = new Row();
                foreach (string column in groupBy)
                {
                    currentAggregate[column] = row[column];
                }

                Accumulate(row, currentAggregate);
            }

            if (currentAggregate != null)
            {
                FinishAggregation(currentAggregate);
                yield return currentAggregate;
            }

            yield break;
        }
    }
}