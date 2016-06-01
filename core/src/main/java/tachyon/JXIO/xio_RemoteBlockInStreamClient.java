package tachyon.JXIO;

import org.accelio.jxio.*;
import org.accelio.jxio.exceptions.JxioGeneralException;
import org.accelio.jxio.exceptions.JxioQueueOverflowException;
import org.accelio.jxio.exceptions.JxioSessionClosedException;
import org.apache.log4j.Logger;
import tachyon.Constants;

import tachyon.worker.DataServerMessage;
import tachyon.conf.UserConf;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;

/**
* Created by yuyang on 2014/10/8.
*/
public class xio_RemoteBlockInStreamClient{

    private int BufferSize;        //构造函数初始化的时候传入BufferSize的大小
    private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

    private MsgPool mp;                       //定义一个MsgPool用于收发消息和数据
    private EventQueueHandler eqh;            //用于处理消息回调
    private ClientSession RemoteBLockInStreamClientSession;//由于这个类作为请求访问block的客户端，因此这个类应该建立ClientSession

    private ByteBuffer mRecvResponseFromDataServer;                //接收到的消息
    private volatile boolean IsRecvResponseFromDataServer;  //用来标志是否已经接收到数据了,注意这个不反应消息的完整性

    //维持连接的增加字段
    private URI uri;
    private volatile boolean isConnected;

    /**
     * RemoteBlockInStreamClient的构造器
     * @param BufferSize
     */
    private xio_RemoteBlockInStreamClient(int BufferSize){
        this.BufferSize = BufferSize;
        this.mp = new MsgPool(1,xio_DataServerMessage.HEADER_LENGTH+BufferSize,xio_DataServerMessage.HEADER_LENGTH);
        this.eqh = new EventQueueHandler(null);
        mRecvResponseFromDataServer = null;                //接收到的消息
        IsRecvResponseFromDataServer = false;

        this.uri = uri;
        this.isConnected = false;
    }

    /**
     * 这里实现为单例模式，这个类主要负责维持与DataServer之间的连接，并且负责传送数据
     * @param RecvData
     * @return
     */

    private static xio_RemoteBlockInStreamClient instance = null;

    public static xio_RemoteBlockInStreamClient getInstance(){
        if(null == instance){
            instance = new xio_RemoteBlockInStreamClient(UserConf.get().REMOTE_READ_BUFFER_SIZE_BYTE);
        }

        return instance;
    }

    private synchronized boolean setRecvResponseFromDataServer(ByteBuffer RecvData){
        mRecvResponseFromDataServer = null;
        try{
            mRecvResponseFromDataServer = ByteBuffer.allocate(RecvData.capacity());
        }catch (IllegalArgumentException e){
            e.printStackTrace();
            return false;
        }

        mRecvResponseFromDataServer.clear();
        mRecvResponseFromDataServer.put(RecvData);
        mRecvResponseFromDataServer.flip();

        return true;
    }

    private synchronized void SetIsRecvData(boolean IsRecvData){
        IsRecvResponseFromDataServer = IsRecvData;
    }
    private boolean IsRecvData() {
        return IsRecvResponseFromDataServer;
    }

    /**
     * ConnectDataServer:连接DataServer服务器的接口
     * @param address
     * @return
     * 注意事项：由于该函数内部调用的SendRequest是异步的，因此，该函数返回true以后，也不意味着消息已经成功发送。
     */
    private boolean ConnectDataServer(InetSocketAddress address) {
        //现将InetSocketAddress转换成URI
        URI uri;
        try {
            uri = new URI("rdma://" + address.getAddress().getHostAddress() + ":" + address.getPort() + "/");
        } catch (URISyntaxException e) {
            e.printStackTrace();
            this.isConnected = false;
            return false;
        }

        if (this.uri == null || this.uri.compareTo(uri) != 0 || this.RemoteBLockInStreamClientSession.getIsClosing()) {
            if (this.uri != null && this.uri != uri) {
                cleanConnection();
            }
            this.uri = uri;
            LOG.info("Connecting the DataServer :" + this.uri.toString());
            this.RemoteBLockInStreamClientSession = new ClientSession(eqh, uri, new RemoteBlockInStreamCallbacks(this));
            this.eqh.runEventLoop(1, EventQueueHandler.INFINITE_DURATION);
        }

        return this.isConnected;
    }

    private boolean cleanConnection() {
        if(this.RemoteBLockInStreamClientSession.getIsClosing()) {
            this.isConnected = false;
        }
        else {
            if(this.RemoteBLockInStreamClientSession.close()) {
                this.eqh.runEventLoop(1,EventQueueHandler.INFINITE_DURATION);

                if(!this.isConnected) {
                    this.isConnected = false;
                }
                else {
                    if(RemoteBLockInStreamClientSession.getIsClosing()) {
                        this.isConnected = false;
                    }
                    else {
                        this.isConnected = true;
                    }
                }
            }
            else {
                isConnected = !RemoteBLockInStreamClientSession.getIsClosing();
            }
        }
        return this.isConnected;
    }

    private boolean sendRequest(long blockId,long offset,long length){
        LOG.info("Send request the block:" + blockId + "offset:" + offset + "length:" + length);
        //封装一个请求包，这里调用DataServerMessage获得一个请求格式的包，然后对DataServerMessage进行解析以后，重新封装
        DataServerMessage sendMsg = DataServerMessage.createBlockRequestMessage(blockId, offset, length);
        //重新封装数据包
        ByteBuffer xio_SendMsg = xio_DataServerMessage.CreateBlockRequestMessagePacket(sendMsg.getBlockId(), sendMsg.getOffset(), sendMsg.getLength(), -1);

        //发送请求包
        Msg msg = this.mp.getMsg();
        msg.getOut().put(xio_SendMsg);

        try {
            RemoteBLockInStreamClientSession.sendRequest(msg);
        }catch(JxioGeneralException e){
            return false;
        }catch(JxioSessionClosedException e){
            mp.releaseMsg(msg);
            return false;
        }catch (JxioQueueOverflowException e){
            return false;
        }

        return true;
    }

    public ByteBuffer ReadBlockFromRemoteMachine(InetSocketAddress address,long blockId){
        return this.ReadBlockFromRemoteMachine(address,blockId,0,-1);
    }
    public ByteBuffer ReadBlockFromRemoteMachine(InetSocketAddress address,long blockId,long offset,long length){

        if( !ConnectDataServer(address) || !sendRequest(blockId,offset,length) ){
            return null;
        }

        this.eqh.runEventLoop(1,EventQueueHandler.INFINITE_DURATION);

        if(IsRecvData())
        {
            //把收到的消息包拆开，提取去Data部分返回
            return xio_DataServerMessage.GetData(mRecvResponseFromDataServer);
        }
        else
        {
            return null;
        }
    }


    /**
     * 以下是ClientSession的回调接口的实现
     */
    class RemoteBlockInStreamCallbacks implements ClientSession.Callbacks{
        private xio_RemoteBlockInStreamClient client;

        RemoteBlockInStreamCallbacks(xio_RemoteBlockInStreamClient client){
            this.client = client;
        }

        public void onSessionEstablished(){
            LOG.info("ClientSession has established!");
            isConnected = true;
        }

        public void onMsgError(Msg msg,EventReason reason){
            LOG.error("A msg error has occurred! because of" + reason);
            msg.returnToParentPool();
            client.SetIsRecvData(false);
        }

        public void onResponse(Msg msg){
            client.SetIsRecvData(client.setRecvResponseFromDataServer(msg.getIn()));     //表示已经接受到消息了
            msg.returnToParentPool();
        }

        public void onSessionEvent(EventName event,EventReason reason){
            if(event == EventName.SESSION_CLOSED){
                LOG.info("Session has closed!");
                uri = null;
                isConnected = false;
                client.RemoteBLockInStreamClientSession = null;
            }

            if(event == EventName.SESSION_REJECT){
                LOG.info("Server reject to receive the session,because of" + reason.toString());
                isConnected = false;
            }

            if(event == EventName.SESSION_ERROR){
                LOG.error("Session has occurred a error,because of" + reason.toString());
                client.RemoteBLockInStreamClientSession.close();
                isConnected = false;
            }
        }
    }
}