/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
 */
package com.ibm.streamsx.test.samples.javaprimitives.ops;

import static com.ibm.streams.operator.StreamingData.Punctuation.FINAL_MARKER;
import static com.ibm.streams.operator.StreamingData.Punctuation.WINDOW_MARKER;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.samples.patterns.ProcessTupleProducer;

/**
 * Generator a sequence of tuples from 0 to N.
 * 
 * Simple source to demonstrate testing of a Java Primitive source
 * using IBM Streams Java mock operator framework.
 * 
 * The output schema must have a {@code int32} as the
 * first attribute.
 *
 */
@PrimitiveOperator
@OutputPortSet
public class SequenceGenerator extends ProcessTupleProducer {
	
	private int total = 20;

	@Override
	protected void process() throws Exception {
		StreamingOutput<OutputTuple> port = getOutput(0);
		
		for (int i = 0; i < getTotal(); i++) {
			port.submitAsTuple(i);
		}
		
		port.punctuate(WINDOW_MARKER);
		port.punctuate(FINAL_MARKER);
	}

	public int getTotal() {
		return total;
	}

	@Parameter(optional=true)
	public void setTotal(int total) {
		this.total = total;
	}
}
