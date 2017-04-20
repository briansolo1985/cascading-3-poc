package com.ferenk.bigdata;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.google.common.base.Strings;
import com.google.common.primitives.Ints;

public class FillDate extends BaseOperation<Tuple> implements Function<Tuple> {

  private static final long serialVersionUID = 1L;

  private static final int SATURDAY = 6;

  private static final DateTimeFormatter INPUT_FORMAT = DateTimeFormat.forPattern("yyyy.MM.dd.");
  private static final DateTimeFormatter OUTPUT_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd");

  private final Fields dateField;
  private final Fields yearField;
  private final Fields weekField;

  public FillDate(Fields dateField, Fields yearField, Fields weekField) {
    super(Fields.ARGS);
    this.dateField = dateField;
    this.yearField = yearField;
    this.weekField = weekField;
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Tuple> operationCall) {
    // TODO create tupleentry here
    operationCall.setContext(Tuple.size(1));
  }

  @Override
  public void operate(@SuppressWarnings("rawtypes") FlowProcess flowProcess, FunctionCall<Tuple> functionCall) {
    TupleEntry tupleEntry = functionCall.getArguments();
    String date = tupleEntry.getString(dateField);

    // TODO reuse tupleEntry from context
    tupleEntry = new TupleEntry(tupleEntry.getFields(), tupleEntry.getTupleCopy());
    DateTime newDate = Strings.isNullOrEmpty(date) ? calculateDate(tupleEntry) : INPUT_FORMAT.parseDateTime(date);
    tupleEntry.setString(dateField, OUTPUT_FORMAT.print(newDate));
    functionCall.getOutputCollector().add(tupleEntry);
  }

  private DateTime calculateDate(TupleEntry tupleEntry) {
    int week = Ints.tryParse(tupleEntry.getString(weekField));
    return new DateTime()
        .withYear(Ints.tryParse(tupleEntry.getString(yearField)))
        .withWeekOfWeekyear(week <= 52 ? week : 52)
        .withDayOfWeek(SATURDAY);
  }
}
