/*
 * Copyright (C) 2007-2020 Paolo Boldi and Sebastiano Vigna
 *
 * This program and the accompanying materials are made available under the
 * terms of the GNU Lesser General Public License v2.1 or later,
 * which is available at
 * http://www.gnu.org/licenses/old-licenses/lgpl-2.1-standalone.html,
 * or the Apache Software License 2.0, which is available at
 * https://www.apache.org/licenses/LICENSE-2.0.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later OR Apache-2.0
 */

package it.unimi.dsi.webgraph.labelling;

import java.io.IOException;
import java.util.NoSuchElementException;

import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.FlyweightPrototype;
import it.unimi.dsi.lang.ObjectParser;

/** A set of attributes that can be used to decorate a node or
 *  an arc of a graph. Attributes appear in the form of &lt;<var>key</var>,<var>value</var>&gt;
 *  pairs, where keys are of type {@link String}. Among attributes,
 *  one (called the <em>well-known attribute</em>), has a special status:
 *  its key can be obtained by using the {@link #wellKnownAttributeKey()} method.
 *
 *  <p>Values associated to attributes can be anything: the value can be
 *  obtained (in the form of an object) with {@link #get(String)}.
 *  If the value is of primitive type, the alternative type-specific method
 *  (e.g., {@link #getInt(String)}, or {@link #getChar(String)}) can be
 *  called, with the proviso that such methods may throw an {@link java.lang.IllegalArgumentException}
 *  if the attribute type can not be converted to the one specified without loss of information.
 *
 *  <p>The value of the well-known attribute can be obtained with {@link #get()},
 *  or with the appropriate type-specific version of the method.
 *
 *  <h2>Serialisation</h2>
 *
 *  <p>Implementations must provide {@link #toBitStream(OutputBitStream, int)} and {@link #fromBitStream(InputBitStream, int)}
 *  methods that serialise to a bitstream and deserialise to a bitstream a label, respectively. Since
 *  {@link #fromBitStream(InputBitStream, int)} has no length information, the label format must
 *  be self-delimiting. This can be obtained with a fixed length scheme (see, e.g., {@link FixedWidthIntLabel}),
 *  or using self-delimiting codes (see, e.g., {@link GammaCodedIntLabel}).
 *
 *  <p>The methods {@link #toBitStream(OutputBitStream,int)}
 *  and {@link #fromBitStream(InputBitStream,int)} are given as an additional information the number of source
 *  node of the arc over which this label is put. They may use this information to decide how the
 *  label should be stored (typically, to do a more clever compression job).
 *
 *  <p>The advantage of fixed-width labels (i.e., those for which {@link #fixedWidth()} does not return -1)
 *  is that when loading a {@link BitStreamArcLabelledImmutableGraph} with an offset step larger than 1 the position in the bitstream
 *  for the labels of a node can be calculated more quickly, as the computation just requires the outdegree
 *  of the nodes, whereas in general one has to skip in-between labels with an explicit deserialisation.
 *
 *  <h2>String-based constructors</h2>
 *
 *  <p>By convention, all concrete classes implementing this interface must follow the {@link ObjectParser} conventions:
 *  in particular, they must provide a constructor accepting strings (either in fixed or variable number) where the first string is the key.
 *  The constructor must perform data validation and build an instance with a default value (e.g., 0 for numerical labels). The
 *  constructor is used, for instance, by {@link BitStreamArcLabelledImmutableGraph} to instantiate a label prototype.
 *  Finally, the method {@link #toSpec()} must return a string that is accepted by {@link ObjectParser}.
 */


public interface Label extends FlyweightPrototype<Label> {
	public static final Label[] EMPTY_LABEL_ARRAY = new Label[0];

	/** Returns the well-known attribute key.
	 *
	 * @return the well-known attribute key.
	 */
	public String wellKnownAttributeKey();

	/** All attribute keys (in arbitrary order).
	 *
	 * @return the keys of all attributes.
	 */
	public String[] attributeKeys();

	/** The types of all attributes in the same order as they are returned by {@link #attributeKeys()}.
	 *
	 * @return the type of all attributes.
	 */
	public Class<?>[] attributeTypes();

	/** The value associated to the attribute with given key.
	 *
	 * @param key the attribute key.
	 * @return the attribute value; if the attribute type is primitive, it is wrapped suitably.
	 * @throws NoSuchElementException if the attribute key is not one of the attributes of this label.
	 */
	public Object get(String key) throws NoSuchElementException;

	/** The value associated to the attribute with given key, provided that the latter has a type that fits a byte.
	 *  Otherwise, an {@link IllegalArgumentException} is thrown.
	 *
	 * @param key the attribute key.
	 * @return the attribute value; if the attribute type is primitive, it is wrapped suitably.
	 * @throws IllegalArgumentException if the attribute key is not known, or it has the wrong type.
	 */
	public byte getByte(String key) throws IllegalArgumentException;

	/** The value associated to the attribute with given key, provided that the latter has a type that fits a short.
	 *  Otherwise, an {@link IllegalArgumentException} is thrown.
	 *
	 * @param key the attribute key.
	 * @return the attribute value; if the attribute type is primitive, it is wrapped suitably.
	 * @throws IllegalArgumentException if the attribute key is not known, or it has the wrong type.
	 */
	public short getShort(String key) throws IllegalArgumentException;

	/** The value associated to the attribute with given key, provided that the latter has a type that fits a int.
	 *  Otherwise, an {@link IllegalArgumentException} is thrown.
	 *
	 * @param key the attribute key.
	 * @return the attribute value; if the attribute type is primitive, it is wrapped suitably.
	 * @throws IllegalArgumentException if the attribute key is not known, or it has the wrong type.
	 */
	public int getInt(String key) throws IllegalArgumentException;

	/** The value associated to the attribute with given key, provided that the latter has a type that fits a long.
	 *  Otherwise, an {@link IllegalArgumentException} is thrown.
	 *
	 * @param key the attribute key.
	 * @return the attribute value; if the attribute type is primitive, it is wrapped suitably.
	 * @throws IllegalArgumentException if the attribute key is not known, or it has the wrong type.
	 */
	public long getLong(String key) throws IllegalArgumentException;

	/** The value associated to the attribute with given key, provided that the latter has a type that fits a float.
	 *  Otherwise, an {@link IllegalArgumentException} is thrown.
	 *
	 * @param key the attribute key.
	 * @return the attribute value; if the attribute type is primitive, it is wrapped suitably.
	 * @throws IllegalArgumentException if the attribute key is not known, or it has the wrong type.
	 */
	public float getFloat(String key) throws IllegalArgumentException;

	/** The value associated to the attribute with given key, provided that the latter has a type that fits a double.
	 *  Otherwise, an {@link IllegalArgumentException} is thrown.
	 *
	 * @param key the attribute key.
	 * @return the attribute value; if the attribute type is primitive, it is wrapped suitably.
	 * @throws IllegalArgumentException if the attribute key is not known, or it has the wrong type.
	 */
	public double getDouble(String key) throws IllegalArgumentException;

	/** The value associated to the attribute with given key, provided that the latter has a type that fits a char.
	 *  Otherwise, an {@link IllegalArgumentException} is thrown.
	 *
	 * @param key the attribute key.
	 * @return the attribute value; if the attribute type is primitive, it is wrapped suitably.
	 * @throws IllegalArgumentException if the attribute key is not known, or it has the wrong type.
	 */
	public char getChar(String key) throws IllegalArgumentException;

	/** The value associated to the attribute with given key, provided that the latter has a type that fits a boolean.
	 *  Otherwise, an {@link IllegalArgumentException} is thrown.
	 *
	 * @param key the attribute key.
	 * @return the attribute value; if the attribute type is primitive, it is wrapped suitably.
	 * @throws IllegalArgumentException if the attribute key is not known, or it has the wrong type.
	 */
	public boolean getBoolean(String key) throws IllegalArgumentException;

	/** The value associated to the well-known attribute.
	 *
	 * @return the attribute value; if the attribute type is primitive, it is wrapped suitably.
	 */
	public Object get() throws NoSuchElementException;

	/** The value associated to the well-known attribute, provided that the latter has a type that fits a byte.
	 *  Otherwise, an {@link IllegalArgumentException} is thrown.
	 *
	 * @return the attribute value; if the attribute type is primitive, it is wrapped suitably.
	 * @throws IllegalArgumentException if the attribute key is not known, or it has the wrong type.
	 */
	public byte getByte() throws IllegalArgumentException;

	/** The value associated to the well-known attribute, provided that the latter has a type that fits a short.
	 *  Otherwise, an {@link IllegalArgumentException} is thrown.
	 *
	 * @return the attribute value; if the attribute type is primitive, it is wrapped suitably.
	 * @throws IllegalArgumentException if the attribute key is not known, or it has the wrong type.
	 */
	public short getShort() throws IllegalArgumentException;

	/** The value associated to the well-known attribute, provided that the latter has a type that fits a int.
	 *  Otherwise, an {@link IllegalArgumentException} is thrown.
	 *
	 * @return the attribute value; if the attribute type is primitive, it is wrapped suitably.
	 * @throws IllegalArgumentException if the attribute key is not known, or it has the wrong type.
	 */
	public int getInt() throws IllegalArgumentException;

	/** The value associated to the well-known attribute, provided that the latter has a type that fits a long.
	 *  Otherwise, an {@link IllegalArgumentException} is thrown.
	 *
	 * @return the attribute value; if the attribute type is primitive, it is wrapped suitably.
	 * @throws IllegalArgumentException if the attribute key is not known, or it has the wrong type.
	 */
	public long getLong() throws IllegalArgumentException;

	/** The value associated to the well-known attribute, provided that the latter has a type that fits a float.
	 *
	 * @return the attribute value; if the attribute type is primitive, it is wrapped suitably.
	 * @throws IllegalArgumentException if the attribute key is not known, or it has the wrong type.
	 */
	public float getFloat() throws IllegalArgumentException;

	/** The value associated to the well-known attribute, provided that the latter has a type that fits a double.
	 *  Otherwise, an {@link IllegalArgumentException} is thrown.
	 *
	 * @return the attribute value; if the attribute type is primitive, it is wrapped suitably.
	 * @throws IllegalArgumentException if the attribute key is not known, or it has the wrong type.
	 */
	public double getDouble() throws IllegalArgumentException;

	/** The value associated to the well-known attribute, provided that the latter has a type that fits a char.
	 *  Otherwise, an {@link IllegalArgumentException} is thrown.
	 *
	 * @return the attribute value; if the attribute type is primitive, it is wrapped suitably.
	 * @throws IllegalArgumentException if the attribute key is not known, or it has the wrong type.
	 */
	public char getChar() throws IllegalArgumentException;

	/** The value associated to the well-known attribute, provided that the latter has a type that fits a boolean.
	 *  Otherwise, an {@link IllegalArgumentException} is thrown.
	 *
	 * @return the attribute value; if the attribute type is primitive, it is wrapped suitably.
	 * @throws IllegalArgumentException if the attribute key is not known, or it has the wrong type.
	 */
	public boolean getBoolean() throws IllegalArgumentException;

	/** Returns a copy of this label.
	 *
	 * @return a new label that copies this one.
	 */
	@Override
	public Label copy();

	/** Returns a string representing the specification of this label.
	 *
	 * <p>Each label class can be instantiated in several ways (e.g., {@link FixedWidthIntLabel}
	 * requires a name for the well-known attribute and a number of bits). This method must return
	 * a representation that can be used by {@link ObjectParser} to instantiate the class, and
	 * consequently there <strong>must</strong> exist a matching constructor whose arguments are strings.
	 *
	 * <p>There is an equation that must be always satisfied:
	 * <pre style="text-align:center; padding: .5em">
	 * ObjectParser.fromSpec(x.toSpec()).toSpec().equals(x.toSpec())
	 * </pre>
	 * @return a string representing the specification of this label.
	 * @see ObjectParser#fromSpec(String, Class)
	 */
	public String toSpec();

	/** Fills this label with data from the given input bit stream, knowing the source node of the arc.
	 *  If {@link #fixedWidth()} is not negative, the value returned must coincide with {@link #fixedWidth()}.
	 *  This method is optional.
	 *
	 * @param inputBitStream an input bit stream offering a label.
	 * @param source the source node.
	 * @return the number of bits read to fill this label.
	 */
	public int fromBitStream(InputBitStream inputBitStream, int source) throws IOException, UnsupportedOperationException;

	/** Writes out this label to the given input bit stream, in self-delimiting form, knowing the source node of the arc.
	 *  If {@link #fixedWidth()} is not negative, the value returned must coincide with {@link #fixedWidth()}.
	 *  This method is optional.
	 *
	 * @param outputBitStream an output bit stream where the label will be written.
	 * @param source the source node.
	 * @return the number of bits written.
	 */
	public int toBitStream(OutputBitStream outputBitStream, int source) throws IOException, UnsupportedOperationException;

	/** Returns the fixed length of this label, in bits, if this label has fixed width.
	 *
	 * @return  the fixed length of this label, or -1 if this label has not fixed width.
	 */
	public int fixedWidth();
}
