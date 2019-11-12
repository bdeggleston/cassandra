/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.marshal;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.MarshalException;

import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.values.Value;
import org.apache.cassandra.utils.values.Values;
import org.github.jamm.Unmetered;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.marshal.AbstractType.ComparisonType.CUSTOM;

/**
 * Specifies a Comparator for a specific type of ByteBuffer.
 *
 * Note that empty ByteBuffer are used to represent "start at the beginning"
 * or "stop at the end" arguments to get_slice, so the Comparator
 * should always handle those values even if they normally do not
 * represent a valid ByteBuffer for the type being compared.
 */
@Unmetered
public abstract class AbstractType<T> implements Comparator<ByteBuffer>, AssignmentTestable
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractType.class);

    public final Comparator<ByteBuffer> reverseComparator;

    public enum ComparisonType
    {
        /**
         * This type should never be compared
         */
        NOT_COMPARABLE,
        /**
         * This type is always compared by its sequence of unsigned bytes
         */
        BYTE_ORDER,
        /**
         * This type can only be compared by calling the type's compareCustom() method, which may be expensive.
         * Support for this may be removed in a major release of Cassandra, however upgrade facilities will be
         * provided if and when this happens.
         */
        CUSTOM
    }

    public final ComparisonType comparisonType;
    public final boolean isByteOrderComparable;

    protected AbstractType(ComparisonType comparisonType)
    {
        this.comparisonType = comparisonType;
        this.isByteOrderComparable = comparisonType == ComparisonType.BYTE_ORDER;
        reverseComparator = (o1, o2) -> AbstractType.this.compare(o2, o1);
        try
        {
            Method custom = getClass().getMethod("compareCustom", Object.class, Object.class, DataHandle.class);
            if ((custom.getDeclaringClass() == AbstractType.class) == (comparisonType == CUSTOM))
                throw new IllegalStateException((comparisonType == CUSTOM ? "compareCustom must be overridden if ComparisonType is CUSTOM"
                                                                         : "compareCustom should not be overridden if ComparisonType is not CUSTOM")
                                                + " (" + getClass().getSimpleName() + ")");
        }
        catch (NoSuchMethodException e)
        {
            throw new IllegalStateException();
        }
    }

    static <V, T extends Comparable<T>> int compareComposed(V left, V right, DataHandle<V> handle, AbstractType<T> type)
    {
        if (handle.isEmpty(left) || handle.isEmpty(right))
            return Boolean.compare(handle.isEmpty(right), handle.isEmpty(left));

        return type.compose(left, handle).compareTo(type.compose(right, handle));
    }

    public static List<String> asCQLTypeStringList(List<AbstractType<?>> abstractTypes)
    {
        List<String> r = new ArrayList<>(abstractTypes.size());
        for (AbstractType<?> abstractType : abstractTypes)
            r.add(abstractType.asCQL3Type().toString());
        return r;
    }

    public final T compose(ByteBuffer bytes)
    {
        return getSerializer().deserialize(bytes);
    }

    public final T compose(Value value)
    {
        return getSerializer().deserialize(value);
    }

    <V> T compose(V value, DataHandle<V> handle)
    {
        return getSerializer().deserialize(value, handle);
    }


    public final ByteBuffer decomposeBuffer(T value)
    {
        return decompose(value, ByteBufferHandle.instance);
    }

    public final Value decomposeValue(T value)
    {
        return decompose(value, ValueHandle.instance);
    }

    <V> V decompose(T value, DataHandle<V> handle)
    {
        return getSerializer().serialize(value, handle);
    }

    /** get a string representation of the bytes used for various identifier (NOT just for log messages) */
    public <V> String getString(V value, DataHandle<V> handle)
    {
        if (value == null)
            return "null";

        TypeSerializer<T> serializer = getSerializer();
        serializer.validate(value, handle);

        return serializer.toString(serializer.deserialize(value, handle));
    }

    public final String getString(ByteBuffer bytes)
    {
        return getString(bytes, ByteBufferHandle.instance);
    }

    public final String getString(Value value)
    {
        return getString(value, ValueHandle.instance);
    }

    /** get a byte representation of the given string. */
    public abstract ByteBuffer fromString(String source) throws MarshalException;

    public Value valueFromString(String source) throws MarshalException
    {
        // FIXME: convert to Value or use handle
        return Values.valueOf(fromString(source));
    }

    /** Given a parsed JSON string, return a byte representation of the object.
     * @param parsed the result of parsing a json string
     **/
    public abstract Term fromJSONObject(Object parsed) throws MarshalException;

    /**
     * Converts the specified value into its JSON representation.
     * <p>
     * The buffer position will stay the same.
     * </p>
     *
     * @param buffer the value to convert
     * @param protocolVersion the protocol version to use for the conversion
     * @return a JSON string representing the specified value
     */
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return '"' + getSerializer().deserialize(buffer).toString() + '"';
    }

    /* validate that the byte array is a valid sequence for the type we are supposed to be comparing */
    public void validate(ByteBuffer bytes) throws MarshalException
    {
        getSerializer().validate(bytes);
    }

    /* validate that the value is a valid sequence of bytes for the type we are supposed to be comparing */
    public void validate(Value value) throws MarshalException
    {
        getSerializer().validate(value);
    }

    public final int compare(ByteBuffer left, ByteBuffer right)
    {
        return compare(left, right, ByteBufferHandle.instance);
    }

    public final int compare(Value left, Value right)
    {
        return compare(left, right, ValueHandle.instance);
    }

    protected final <V> int compare(V left, V right, DataHandle<V> handle)
    {
        return isByteOrderComparable ? handle.compareUnsigned(left, right) : compareCustom(left, right, handle);
    }

    /**
     * Implement IFF ComparisonType is CUSTOM
     *
     * Compares the byte representation of two instances of this class,
     * for types where this cannot be done by simple in-order comparison of the
     * unsigned bytes
     *
     * Standard Java compare semantics
     */
    public <V> int compareCustom(V left, V right, DataHandle<V> handle)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Validate cell value. Unlike {@linkplain #validate(java.nio.ByteBuffer)},
     * cell value is passed to validate its content.
     * Usually, this is the same as validate except collection.
     *
     * @param cellValue ByteBuffer representing cell value
     * @throws MarshalException
     */
    public void validateCellValue(Value cellValue) throws MarshalException
    {
        validate(cellValue);
    }

    /* Most of our internal type should override that. */
    public CQL3Type asCQL3Type()
    {
        return new CQL3Type.Custom(this);
    }

    /**
     * Same as compare except that this ignore ReversedType. This is to be use when
     * comparing 2 values to decide for a CQL condition (see Operator.isSatisfiedBy) as
     * for CQL, ReversedType is simply an "hint" to the storage engine but it does not
     * change the meaning of queries per-se.
     */
    public int compareForCQL(ByteBuffer v1, ByteBuffer v2)
    {
        return compare(v1, v2);
    }

    public abstract TypeSerializer<T> getSerializer();

    /* convenience method */
    public String getString(Collection<ByteBuffer> names)
    {
        StringBuilder builder = new StringBuilder();
        for (ByteBuffer name : names)
        {
            builder.append(getString(name)).append(",");
        }
        return builder.toString();
    }

    public boolean isCounter()
    {
        return false;
    }

    public boolean isFrozenCollection()
    {
        return isCollection() && !isMultiCell();
    }

    public boolean isReversed()
    {
        return false;
    }

    public static AbstractType<?> parseDefaultParameters(AbstractType<?> baseType, TypeParser parser) throws SyntaxException
    {
        Map<String, String> parameters = parser.getKeyValueParameters();
        String reversed = parameters.get("reversed");
        if (reversed != null && (reversed.isEmpty() || reversed.equals("true")))
        {
            return ReversedType.getInstance(baseType);
        }
        else
        {
            return baseType;
        }
    }

    /**
     * Returns true if this comparator is compatible with the provided
     * previous comparator, that is if previous can safely be replaced by this.
     * A comparator cn should be compatible with a previous one cp if forall columns c1 and c2,
     * if   cn.validate(c1) and cn.validate(c2) and cn.compare(c1, c2) == v,
     * then cp.validate(c1) and cp.validate(c2) and cp.compare(c1, c2) == v.
     *
     * Note that a type should be compatible with at least itself and when in
     * doubt, keep the default behavior of not being compatible with any other comparator!
     */
    public boolean isCompatibleWith(AbstractType<?> previous)
    {
        return this.equals(previous);
    }

    /**
     * Returns true if values of the other AbstractType can be read and "reasonably" interpreted by the this
     * AbstractType. Note that this is a weaker version of isCompatibleWith, as it does not require that both type
     * compare values the same way.
     *
     * The restriction on the other type being "reasonably" interpreted is to prevent, for example, IntegerType from
     * being compatible with all other types.  Even though any byte string is a valid IntegerType value, it doesn't
     * necessarily make sense to interpret a UUID or a UTF8 string as an integer.
     *
     * Note that a type should be compatible with at least itself.
     */
    public boolean isValueCompatibleWith(AbstractType<?> otherType)
    {
        return isValueCompatibleWithInternal((otherType instanceof ReversedType) ? ((ReversedType) otherType).baseType : otherType);
    }

    /**
     * Needed to handle ReversedType in value-compatibility checks.  Subclasses should implement this instead of
     * isValueCompatibleWith().
     */
    protected boolean isValueCompatibleWithInternal(AbstractType<?> otherType)
    {
        return isCompatibleWith(otherType);
    }

    /**
     * An alternative comparison function used by CollectionsType in conjunction with CompositeType.
     *
     * This comparator is only called to compare components of a CompositeType. It gets the value of the
     * previous component as argument (or null if it's the first component of the composite).
     *
     * Unless you're doing something very similar to CollectionsType, you shouldn't override this.
     */
    public <V> int compareCollectionMembers(V v1, V v2, V collectionName, DataHandle<V> handle)
    {
        return compare(v1, v2, handle);
    }

    /**
     * An alternative validation function used by CollectionsType in conjunction with CompositeType.
     *
     * This is similar to the compare function above.
     */
    public void validateCollectionMember(ByteBuffer bytes, ByteBuffer collectionName) throws MarshalException
    {
        validate(bytes);
    }

    public <V> void validateCollectionMember(V value, V collectionName, DataHandle<V> handle) throws MarshalException
    {
        getSerializer().validate(value, handle);
    }


    public boolean isCollection()
    {
        return false;
    }

    public boolean isUDT()
    {
        return false;
    }

    public boolean isTuple()
    {
        return false;
    }

    public boolean isMultiCell()
    {
        return false;
    }

    public boolean isFreezable()
    {
        return false;
    }

    public AbstractType<?> freeze()
    {
        return this;
    }

    /**
     * Returns an AbstractType instance that is equivalent to this one, but with all nested UDTs and collections
     * explicitly frozen.
     *
     * This is only necessary for {@code 2.x -> 3.x} schema migrations, and can be removed in Cassandra 4.0.
     *
     * See CASSANDRA-11609 and CASSANDRA-11613.
     */
    public AbstractType<?> freezeNestedMulticellTypes()
    {
        return this;
    }

    /**
     * Returns {@code true} for types where empty should be handled like {@code null} like {@link Int32Type}.
     */
    public boolean isEmptyValueMeaningless()
    {
        return false;
    }

    /**
     * @param ignoreFreezing if true, the type string will not be wrapped with FrozenType(...), even if this type is frozen.
     */
    public String toString(boolean ignoreFreezing)
    {
        return this.toString();
    }

    /**
     * The number of subcomponents this type has.
     * This is always 1, i.e. the type has only itself as "subcomponents", except for CompositeType.
     */
    public int componentsCount()
    {
        return 1;
    }

    /**
     * Return a list of the "subcomponents" this type has.
     * This always return a singleton list with the type itself except for CompositeType.
     */
    public List<AbstractType<?>> getComponents()
    {
        return Collections.<AbstractType<?>>singletonList(this);
    }

    /**
     * The length of values for this type if all values are of fixed length, -1 otherwise.
     */
    public int valueLengthIfFixed()
    {
        return -1;
    }

    // This assumes that no empty values are passed
    public void writeValue(ByteBuffer value, DataOutputPlus out) throws IOException
    {
        writeValue(value, ByteBufferHandle.instance, out);
    }

    // This assumes that no empty values are passed
    public void writeValue(Value value, DataOutputPlus out) throws IOException
    {
        writeValue(value, ValueHandle.instance, out);
    }

    // This assumes that no empty values are passed
    private <T> void writeValue(T value, DataHandle<T> handle, DataOutputPlus out) throws IOException
    {
        assert !handle.isEmpty(value);
        if (valueLengthIfFixed() >= 0)
            handle.write(value, out);
        else
            handle.writeWithVIntLength(value, out);
    }

    public long writtenLength(ByteBuffer value)
    {
        return writtenLength(value, ByteBufferHandle.instance);
    }

    public long writtenLength(Value value)
    {
        return writtenLength(value, ValueHandle.instance);
    }

    public <T> long writtenLength(T value, DataHandle<T> handle)
    {
        assert !handle.isEmpty(value);
        return valueLengthIfFixed() >= 0
               ? handle.size(value)
               : handle.sizeWithVIntLength(value);
    }

    public ByteBuffer readBuffer(DataInputPlus in) throws IOException
    {
        return readBuffer(in, Integer.MAX_VALUE);
    }

    public ByteBuffer readBuffer(DataInputPlus in, int maxValueSize) throws IOException
    {
        return read(ByteBufferHandle.instance, in, maxValueSize);
    }

    public Value readValue(DataInputPlus in) throws IOException
    {
        return readValue(in, Integer.MAX_VALUE);
    }

    public Value readValue(DataInputPlus in, int maxValueSize) throws IOException
    {
        return read(ValueHandle.instance, in, maxValueSize);
    }

    <T> T read(DataHandle<T> handle, DataInputPlus in, int maxValueSize) throws IOException
    {
        int length = valueLengthIfFixed();

        if (length >= 0)
            return handle.read(in, length);
        else
        {
            int l = (int)in.readUnsignedVInt();
            if (l < 0)
                throw new IOException("Corrupt (negative) value length encountered");

            if (l > maxValueSize)
                throw new IOException(String.format("Corrupt value length %d encountered, as it exceeds the maximum of %d, " +
                                                    "which is set via max_value_size_in_mb in cassandra.yaml",
                                                    l, maxValueSize));

            return handle.read(in, l);
        }
    }

    public void skipValue(DataInputPlus in) throws IOException
    {
        int length = valueLengthIfFixed();
        if (length >= 0)
            in.skipBytesFully(length);
        else
            ByteBufferUtil.skipWithVIntLength(in);
    }

    public boolean referencesUserType(ByteBuffer name)
    {
        return false;
    }

    /**
     * Returns an instance of this type with all references to the provided user type recursively replaced with its new
     * definition.
     */
    public AbstractType<?> withUpdatedUserType(UserType udt)
    {
        return this;
    }

    /**
     * Replace any instances of UserType with equivalent TupleType-s.
     *
     * We need it for dropped_columns, to allow safely dropping unused user types later without retaining any references
     * to them in system_schema.dropped_columns.
     */
    public AbstractType<?> expandUserTypes()
    {
        return this;
    }

    public boolean referencesDuration()
    {
        return false;
    }

    /**
     * Tests whether a CQL value having this type can be assigned to the provided receiver.
     */
    public AssignmentTestable.TestResult testAssignment(AbstractType<?> receiverType)
    {
        // testAssignement is for CQL literals and native protocol values, none of which make a meaningful
        // difference between frozen or not and reversed or not.

        if (isFreezable() && !isMultiCell())
            receiverType = receiverType.freeze();

        if (isReversed() && !receiverType.isReversed())
            receiverType = ReversedType.getInstance(receiverType);

        if (equals(receiverType))
            return AssignmentTestable.TestResult.EXACT_MATCH;

        if (receiverType.isValueCompatibleWith(this))
            return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;

        return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
    }

    /**
     * This must be overriden by subclasses if necessary so that for any
     * AbstractType, this == TypeParser.parse(toString()).
     *
     * Note that for backwards compatibility this includes the full classname.
     * For CQL purposes the short name is fine.
     */
    @Override
    public String toString()
    {
        return getClass().getName();
    }

    public void checkComparable()
    {
        switch (comparisonType)
        {
            case NOT_COMPARABLE:
                throw new IllegalArgumentException(this + " cannot be used in comparisons, so cannot be used as a clustering column");
        }
    }

    public final AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
    {
        return testAssignment(receiver.type);
    }
}
