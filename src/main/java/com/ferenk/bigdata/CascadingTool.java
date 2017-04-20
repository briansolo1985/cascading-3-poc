package com.ferenk.bigdata;

import static cascading.tuple.Fields.ALL;

import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowRuntimeProps;
import cascading.flow.tez.Hadoop2TezFlowConnector;
import cascading.fluid.Fluid;
import cascading.fluid.api.assembly.Assembly.AssemblyBuilder;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class CascadingTool {

  private static Fields lotteryInputHeader = new Fields("Year", "Week", "Date", "FiveNumber", "FiveValue",
      "FourNumber", "FourValue", "ThreeNumber", "ThreeValue", "TwoNumber", "TwoValue", "Number1", "Number2", "Number3",
      "Number4", "Number5");
  private static Fields outputHeader = new Fields("Date", "Number1", "Number2", "Number3", "Number4", "Number5");

  private static Fields dateField = new Fields("Date");
  private static Fields yearField = new Fields("Year");
  private static Fields weekField = new Fields("Week");

  public static void main(String[] args) {
    Properties properties = AppProps.appProps()
        .setJarClass(CascadingTool.class)
        .buildProperties();

    properties = FlowRuntimeProps.flowRuntimeProps()
        .setGatherPartitions(4)
        .buildProperties(properties);

    AssemblyBuilder.Start builder = Fluid.assembly();

    // TODO adding fileName
    // TODO filtering out records with illegal week (53) -> other output pipe
    Pipe lotteryPipe = builder
        .startBranch("lotteryPipe")
        .each(ALL).function(new FillDate(dateField, yearField, weekField)).outgoing(Fields.RESULTS)
        .retain(outputHeader)
        .completeBranch();

    Tap<?, ?, ?> inputTap = new Hfs(new TextDelimited(lotteryInputHeader, true, ";", "'"),
        "/home/ferenk/data/otos/input");
    Tap<?, ?, ?> outputTap = new Hfs(new TextDelimited(ALL, false, "\t", "'"), "/home/ferenk/data/otos/output");

    FlowConnector flowConnector = new Hadoop2TezFlowConnector(properties);
    FlowDef flowDef = FlowDef.flowDef()
        .setName("tezFlow")
        .addSource(lotteryPipe, inputTap)
        .addTailSink(lotteryPipe, outputTap);

    flowConnector.connect(flowDef).complete();

    System.exit(0);
  }

}
