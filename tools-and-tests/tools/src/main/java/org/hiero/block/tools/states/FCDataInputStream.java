/*
 * (c) 2016-2019 Swirlds, Inc.
 *
 * This software is the confidential and proprietary information of
 * Swirlds, Inc. ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Swirlds.
 *
 * SWIRLDS MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY OF
 * THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
 * TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE, OR NON-INFRINGEMENT. SWIRLDS SHALL NOT BE LIABLE FOR
 * ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING OR
 * DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 */
package org.hiero.block.tools.states;


import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A drop-in replacement for {@link DataInputStream}, which handles FastCopyable classes specially. It
 * doesn't add any new methods or variables that are public. It is designed for use with the FastCopyable
 * interface, and its use is described there.
 */
public class FCDataInputStream extends DataInputStream {

	// Use {@inheritDoc} when it starts working--right now (9/26/19) doesn't seem to

	/**
	 * Creates a FCDataInputStream that uses the specified
	 * underlying InputStream.
	 *
	 * @param in
	 * 		the specified input stream
	 */
	public FCDataInputStream(InputStream in) {
		super(in);
	}

	//TODO Javadoc
	public byte[] readBytes() throws IOException {
		return readBytes(Integer.MAX_VALUE);
	}

	//TODO Javadoc
	public byte[] readBytes(int maxLength) throws IOException {
		int len = this.readInt();
		if (len < 0) {
			// if length is negative, it's a null value
			return null;
		}
		byte[] bytes = null;
		if (len < maxLength) {
			bytes = new byte[len];
			this.readFully(bytes);
		} else {
			throw new IOException(String.format(
					"Tried to read %d bytes, which is more than limit of %d",
					len, maxLength
			));
		}
		return bytes;
	}

	/**
	 * Reads a String encoded in the Swirlds default charset (UTF8) from the input stream
	 *
	 * @return the String read
	 * @throws IOException
	 * 		thrown if there are any problems during the operation
	 */
	public String readNormalisedString() throws IOException {
		byte data[] = DataStreamUtils.readByteArray(this);
		return CommonUtils.getNormalisedStringFromBytes(data);
	}
}
