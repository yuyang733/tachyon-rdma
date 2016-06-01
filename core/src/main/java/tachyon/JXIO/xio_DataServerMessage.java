package tachyon.JXIO;


import java.nio.ByteBuffer;


/**
 * Created by yuyang on 2014/10/6.
 */
public class xio_DataServerMessage {

    public static final short DATA_SERVER_REQUEST_MESSAGE = 1;
    public static final short DATA_SERVER_RESPONSE_MESSAGE = 2;

    public static final int HEADER_LENGTH = 30;        //最后放置lockId，所以头部增加4个字节

    private static ByteBuffer CreateHeader(short MsgType,long BlockId,long offset,long length,int lockId){
        ByteBuffer PacketHeader = ByteBuffer.allocate(HEADER_LENGTH);
        PacketHeader.clear();

        PacketHeader.putShort(MsgType);
        PacketHeader.putLong(BlockId);
        PacketHeader.putLong(offset);
        PacketHeader.putLong(length);
        PacketHeader.putInt(lockId);

        PacketHeader.flip();    //封装完毕，将其转换为读模式

        return PacketHeader;
    }

    /**
     * 获取数据包的头部
     * @param Packet
     * @return
     */
    public static ByteBuffer GetHeader(ByteBuffer Packet){
        if(Packet.remaining() < HEADER_LENGTH){
            return null;            //数据包不完整
        }

        ByteBuffer PacketHeader = ByteBuffer.allocate(HEADER_LENGTH);
        PacketHeader.clear();
        PacketHeader.putShort(Packet.getShort());   //取得MsgType
        PacketHeader.putLong(Packet.getLong());     //取得BlockId
        PacketHeader.putLong(Packet.getLong());     //取得offset
        PacketHeader.putLong(Packet.getLong());     //取得length
        PacketHeader.putInt(Packet.getInt());
        PacketHeader.flip();        //转换为读模式

        Packet.rewind();

        return PacketHeader;
    }

    /**
     * 从数据包的报头中取得MsgType类型
     * @param PacketHeader
     * @return
     */
    public static short getMsgTypeFromHeader(ByteBuffer PacketHeader){
        if(PacketHeader.remaining() < HEADER_LENGTH){
            return -1;
        }

        short MsgType = PacketHeader.getShort();
        PacketHeader.rewind();

        return MsgType;
    }

    /**
     * 从数据包的报头中取得BlockId的值
     * @param PacketHeader
     * @return 如果BlockId无效的话，则返回-1
     */
    public static long getBlockIdFromHeader(ByteBuffer PacketHeader){
        if(PacketHeader.remaining() < HEADER_LENGTH){
            return -1;
        }

        PacketHeader.getShort();
        long BlockId = PacketHeader.getLong();
        PacketHeader.rewind();

        return BlockId;
    }

    /**
     * 从数据包的报头中取得offset
     * @param PacketHeader
     * @return
     */
    public static long getOffsetFromHeader(ByteBuffer PacketHeader){
        if(PacketHeader.remaining() < HEADER_LENGTH){
            return -1;
        }

        PacketHeader.getShort();
        PacketHeader.getLong();
        long offset = PacketHeader.getLong();   //取得offset
        PacketHeader.rewind();

        return offset;
    }

    /**
     * 从数据包中获得数据长度
     * @param PacketHeader
     * @return
     */
    public static long getLengthFromHeader(ByteBuffer PacketHeader){
        if(PacketHeader.remaining() < HEADER_LENGTH){
            return -1;
        }

        PacketHeader.getShort();
        PacketHeader.getLong();
        PacketHeader.getLong();
        long length = PacketHeader.getLong();
        PacketHeader.rewind();

        return length;

    }

    /**
     * 从数据包的包头中获得LockId
     * @param PacketHeader
     * @return
     */
    public static int getLockIdFromHeader(ByteBuffer PacketHeader){
        if(PacketHeader.remaining() < HEADER_LENGTH){
            return -1;
        }

        PacketHeader.getShort();    //msgType
        PacketHeader.getLong();     //BlockId
        PacketHeader.getLong();     //offset
        PacketHeader.getLong();     //length
        int LockId = PacketHeader.getInt();
        PacketHeader.rewind();

        return LockId;
    }

    /**
     * 获取数据包的数据部分
     * @param Packet
     * @return
     */
    public static ByteBuffer GetData(ByteBuffer Packet){
        if(Packet.remaining() <= HEADER_LENGTH) {
            return null;
        }

        ByteBuffer PacketData = ByteBuffer.allocate(Packet.remaining()-HEADER_LENGTH);
        PacketData.clear();
        byte[] data = new byte[PacketData.capacity()];
        PacketData.put(Packet.array(),HEADER_LENGTH,PacketData.capacity());
        PacketData.flip();
        Packet.rewind();

        return PacketData;
    }

    /**
     * CreateResponseMessagePacket:给调用者返回一个JXIO可以进行发送的ByteBuffer类型的回应数据包
     * @param BlockId
     * @param offset
     * @param length
     * @param Data
     * @return
     */
    public static ByteBuffer CreateBlockResponseMessagePacket(long BlockId,long offset,long length,ByteBuffer Data,int lockId){
        //先产生一个封装一个消息头
        ByteBuffer PacketHeader = CreateHeader(DATA_SERVER_RESPONSE_MESSAGE,BlockId,offset,length,lockId);

        ByteBuffer ResponseMessagePacket = ByteBuffer.allocate(HEADER_LENGTH + Data.capacity());
        ResponseMessagePacket.put(PacketHeader);
        ResponseMessagePacket.put(Data);
        ResponseMessagePacket.flip();

        return ResponseMessagePacket;
    }

    /**
     * CreateRequestMessagePacket：给调用者返回一个JXIO可以进行发送的ByteBuffer类型的请求数据包
     * @param BlockId
     * @param offset
     * @param length
     * @return
     */
    public static ByteBuffer CreateBlockRequestMessagePacket(long BlockId,long offset,long length,int lockId){
        ByteBuffer PacketHeader = CreateHeader(DATA_SERVER_RESPONSE_MESSAGE,BlockId,offset,length,lockId);
        ByteBuffer RequestMessagePacket = ByteBuffer.allocate(HEADER_LENGTH);
        RequestMessagePacket.put(PacketHeader);
        RequestMessagePacket.flip();
        return RequestMessagePacket;
    }
}
