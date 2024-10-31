package com.example;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.CMQC;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQGetMessageOptions;

/**
 * IBMConsumer - Classe responsável por consumir mensagens de uma fila no IBM MQ.
 *
 * Esta classe estabelece conexão com um gerenciador de filas do IBM MQ e lê mensagens de uma fila
 * específica em um loop contínuo, processando-as à medida que chegam.
 * 
 * @author Desenvolvedor
 * @version 1.0
 * @since 2024
 */
public class IBMConsumer {

    private static final String QMGR_SERPRO = "QMSERPRO";
    private static final String CHANNEL_SERPRO = "ADMIN.CHL";
    private static final String CONN_NAME_SERPRO = "localhost(1515)";
    private static final String QUEUE_NAME = "FILA1";

    public static void main(String[] args) {
        MQQueueManager queueManager = null;
        MQQueue queue = null;

        try {
            // Configuração do ambiente MQ
            com.ibm.mq.MQEnvironment.hostname = CONN_NAME_SERPRO.split("\\(")[0];
            com.ibm.mq.MQEnvironment.channel = CHANNEL_SERPRO;
            com.ibm.mq.MQEnvironment.port = Integer.parseInt(CONN_NAME_SERPRO.split("\\(")[1].replace(")", ""));

            // Conexão ao Gerenciador de Filas
            queueManager = new MQQueueManager(QMGR_SERPRO);

            // Acesso à fila
            int openOptions = CMQC.MQOO_INPUT_AS_Q_DEF | CMQC.MQOO_FAIL_IF_QUIESCING;
            queue = queueManager.accessQueue(QUEUE_NAME, openOptions);

            System.out.println("Esperando mensagens na fila: " + QUEUE_NAME);

            // Loop infinito para espera e processamento de mensagens
            while (true) {
                MQMessage message = new MQMessage();
                MQGetMessageOptions gmo = new MQGetMessageOptions();
                gmo.options = CMQC.MQGMO_WAIT | CMQC.MQGMO_FAIL_IF_QUIESCING;
                gmo.waitInterval = CMQC.MQWI_UNLIMITED;

                queue.get(message, gmo);
                String messageText = message.readStringOfByteLength(message.getMessageLength());
                System.out.println("Mensagem recebida: " + messageText);
            }
        } catch (MQException e) {
            System.err.println("Erro no MQ: " + e.getMessage() + ", Código de Erro: " + e.getReason());
        } catch (Exception e) {
            System.err.println("Erro geral: " + e.getMessage());
        } finally {
            try {
                // Fechando a fila e desconectando do Gerenciador de Filas
                if (queue != null) queue.close();
                if (queueManager != null) queueManager.disconnect();
            } catch (MQException e) {
                System.err.println("Erro ao fechar recursos: " + e.getMessage());
            }
        }
    }
}
