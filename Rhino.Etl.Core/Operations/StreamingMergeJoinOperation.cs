using System;
using System.Collections.Generic;
using Rhino.Etl.Core.Enumerables;

namespace Rhino.Etl.Core.Operations
{
    /// <summary>A streaming, non-blocking, merge join.  Left and Right inputs
    /// must be in sorted order for this to work properly.</summary>
    public abstract class StreamingMergeJoinOperation : AbstractOperation
    {
        private readonly PartialProcessOperation left = new PartialProcessOperation();
        private readonly PartialProcessOperation right = new PartialProcessOperation();

        /// <summary>Sets the right part of the join</summary>
        /// <value>The right.</value>
        public StreamingMergeJoinOperation Right(IOperation value)
        {
            right.Register(value);
            return this;
        }

        /// <summary>Sets the left part of the join</summary>
        /// <value>The left.</value>
        public StreamingMergeJoinOperation Left(IOperation value)
        {
            left.Register(value);
            return this;
        }


        /// <summary>
        /// Executes this operation
        /// </summary>
        /// <param name="rows">The rows.</param>
        /// <returns></returns>
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            Initialize();

            Guard.Against(left == null, "Left branch of a join cannot be null");
            Guard.Against(right == null, "Right branch of a join cannot be null");

            var leftEnumerator = (IEnumerator<Row>) new EventRaisingEnumerator(left, left.Execute(null)).GetEnumerator();
            var rightEnumerator = (IEnumerator<Row>) new EventRaisingEnumerator(right, right.Execute(null)).GetEnumerator();

            bool leftHasRows = leftEnumerator.MoveNext();
            bool rightHasRows = rightEnumerator.MoveNext();

            Row leftCurrent, rightCurrent;

            while (leftHasRows || rightHasRows)
            {
                leftCurrent = leftEnumerator.Current;
                rightCurrent = rightEnumerator.Current;

                if (MatchJoinCondition(leftCurrent, rightCurrent))
                {
                    yield return MergeRows(leftCurrent, rightCurrent);
                }

                leftHasRows = leftEnumerator.MoveNext();
                rightHasRows = rightEnumerator.MoveNext();
            }

            yield break;
        }

        /// <summary>Check if the two rows match to the join condition.</summary>
        /// <param name="leftRow">The left row.</param>
        /// <param name="rightRow">The right row.</param>
        /// <returns></returns>
        protected abstract bool MatchJoinCondition(Row leftRow, Row rightRow);

        /// <summary>
        /// Called when a row on the right side was filtered by
        /// the join condition, allow a derived class to perform 
        /// logic associated to that, such as logging
        /// </summary>
        protected virtual void RightOrphanRow(Row row) { }

        /// <summary>
        /// Called when a row on the left side was filtered by
        /// the join condition, allow a derived class to perform 
        /// logic associated to that, such as logging
        /// </summary>
        /// <param name="row">The row.</param>
        protected virtual void LeftOrphanRow(Row row) { }

        /// <summary>Merges the two rows into a single row</summary>
        /// <param name="leftRow">The left row.</param>
        /// <param name="rightRow">The right row.</param>
        /// <returns></returns>
        protected abstract Row MergeRows(Row leftRow, Row rightRow);

        /// <summary>Initializes this instance.</summary>
        protected virtual void Initialize() { }

        /// <summary>Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.</summary>
        public override void Dispose()
        {
            left.Dispose();
            right.Dispose();
        }

        /// <summary>Initializes this instance</summary>
        /// <param name="pipelineExecuter">The current pipeline executer.</param>
        public override void PrepareForExecution(IPipelineExecuter pipelineExecuter)
        {
            left.PrepareForExecution(pipelineExecuter);
            right.PrepareForExecution(pipelineExecuter);
        }

        /// <summary>Gets all errors that occured when running this operation</summary>
        /// <returns></returns>
        public override IEnumerable<Exception> GetAllErrors()
        {
            foreach (Exception error in left.GetAllErrors())
            {
                yield return error;
            }
            foreach (Exception error in right.GetAllErrors())
            {
                yield return error;
            }
        }
    }
}