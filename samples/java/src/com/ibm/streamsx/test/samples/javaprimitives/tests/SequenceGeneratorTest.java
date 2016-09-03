/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
 */
package com.ibm.streamsx.test.samples.javaprimitives.tests;

import static org.junit.Assert.assertEquals;

import java.util.LinkedList;

import org.junit.Test;

import com.ibm.streams.flow.declare.OperatorGraph;
import com.ibm.streams.flow.declare.OperatorGraphFactory;
import com.ibm.streams.flow.declare.OperatorInvocation;
import com.ibm.streams.flow.declare.OutputPortDeclaration;
import com.ibm.streams.flow.handlers.StreamCollector;
import com.ibm.streams.flow.handlers.StreamCounter;
import com.ibm.streams.flow.javaprimitives.JavaOperatorTester;
import com.ibm.streams.flow.javaprimitives.JavaTestableGraph;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.test.samples.javaprimitives.ops.SequenceGenerator;

/**
 * Demonstrate testing of a Java primitive source operator using SequenceGenerator.
 * 
 * <P>
 * <em>Note:</em> Each test is self contained for clarity of the sample.
 * A test class for a real Java primitive operator might refactor code
 * so that common code only exists once.
 * </P>
 * 
 * @see SequenceGenerator
 */
public class SequenceGeneratorTest {
	
	/**
	 * Test the correct number of tuples are produced by default.
	 * Uses the Java mock operator framework to invoke and
	 * test a single invocation of {@link SequenceGenerator}.
	 * 
	 * <P>
	 * This demonstrates using a
	 * {@code com.ibm.streams.flow.handlers.StreamCounter}
	 * attached to an output port to count tuples and
	 * punctuation marks without any tests against the tuple
	 * contents.
	 * </P>
	 */
	@Test
	public void testDefaultSequenceCount() throws Exception {
	
        final OperatorGraph graph = OperatorGraphFactory.newGraph();

        // Declare a SequenceGenerator operator
        OperatorInvocation<SequenceGenerator> sequence = graph
                .addOperator(SequenceGenerator.class);
        
        // With a single output port
        OutputPortDeclaration output = sequence
                .addOutput("tuple<int32 seq>");
              
        // Graph is now declared, create an executable version       
        JavaOperatorTester jot = new JavaOperatorTester();       
        JavaTestableGraph tg = jot.executable(graph);
        
        // Register a handler to count tuples and
        // punctuation marks
        StreamCounter<Tuple> counter = new StreamCounter<Tuple>();        
        tg.registerStreamHandler(output, counter);
        
        // Execute the graph. SequenceGenerator submits
        // a final marker after it has submitted the required
        // number of tuples which will cause the graph to complete.
        tg.executeToCompletion();
        
        // Verify the output (default is twenty tuples).
        assertEquals(20, counter.getTupleCount());
        assertEquals(1, counter.getMarkCount(Punctuation.WINDOW_MARKER));
        assertEquals(1, counter.getMarkCount(Punctuation.FINAL_MARKER));
	}
	
	/**
	 * Test the correct number of tuples are produced by
	 * the {@code total} parameter is set..
	 * Uses the Java mock operator framework to invoke and
	 * test a single invocation of {@link SequenceGenerator}.
	 * Sets the {@code int32 total} parameter to set the
	 * number of generated tuples.
	 * 
	 * <P>
	 * This demonstrates using a
	 * {@code com.ibm.streams.flow.handlers.StreamCounter}
	 * attached to an output port to count tuples and
	 * punctuation marks without any tests against the tuple
	 * contents.
	 * </P>
	 */
	@Test
	public void testSequenceParameterCount() throws Exception {
	
        final OperatorGraph graph = OperatorGraphFactory.newGraph();

        // Declare a SequenceGenerator operator
        OperatorInvocation<SequenceGenerator> sequence = graph
                .addOperator(SequenceGenerator.class);
        
        // With a single output port
        OutputPortDeclaration output = sequence
                .addOutput("tuple<int32 seq>");
        
        // Set the parameter to a different number
        // from the default.
        final int N = 37;       
        sequence.setIntParameter("total", N);
              
        // Graph is now declared, create an executable version       
        JavaOperatorTester jot = new JavaOperatorTester();       
        JavaTestableGraph tg = jot.executable(graph);
        
        // Register a handler to count tuples and
        // punctuation marks
        StreamCounter<Tuple> counter = new StreamCounter<Tuple>();        
        tg.registerStreamHandler(output, counter);
        
        // Execute the graph. SequenceGenerator submits
        // a final marker after it has submitted the required
        // number of tuples which will cause the graph to complete.
        tg.executeToCompletion();
        
        // Verify the output
        assertEquals(N, counter.getTupleCount());
        assertEquals(1, counter.getMarkCount(Punctuation.WINDOW_MARKER));
        assertEquals(1, counter.getMarkCount(Punctuation.FINAL_MARKER));
	}
	
	@Test
	public void testSequenceContents() throws Exception {
	
        final OperatorGraph graph = OperatorGraphFactory.newGraph();

        // Declare a SequenceGenerator operator
        OperatorInvocation<SequenceGenerator> sequence = graph
                .addOperator(SequenceGenerator.class);
        
        // With a single output port
        OutputPortDeclaration output = sequence
                .addOutput("tuple<int32 seq>");
        
        // Set the parameter to a different number
        // from the default.
        final int N = 57;       
        sequence.setIntParameter("total", N);
              
        // Graph is now declared, create an executable version       
        JavaOperatorTester jot = new JavaOperatorTester();       
        JavaTestableGraph tg = jot.executable(graph);
        
        // Register a handler to count tuples and
        // punctuation marks
		StreamCollector<LinkedList<Tuple>, Tuple> collector =
				StreamCollector.newLinkedListCollector();
		tg.registerStreamHandler(output, collector);
        
        // Execute the graph. SequenceGenerator submits
        // a final marker after it has submitted the required
        // number of tuples which will cause the graph to complete.
        tg.executeToCompletion();
        
        // Verify the output
        assertEquals(N, collector.getTupleCount());
        LinkedList<Tuple> tuples = collector.getTuples();
        for (int i = 0; i < N; i++) {
        	assertEquals(i, tuples.get(i).getInt(0));
        }
	}
}
